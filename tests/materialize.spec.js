const State = require("../src/State");
const { strict: assert }  = require("node:assert");
const { Pool } = require("pg");
const { to: copyTo } = require('pg-copy-streams');
const csv = require("csvtojson");
const dotenv = require("dotenv");
const { env } = require('node:process');

dotenv.config();

const { host, user, password } = env;

const config = {
    host,
    port: 6875,
    ssl: true,
    user,
    database: "materialize",
    password
};

///
/// Util functions
///
function clean(row) {
    delete row.mz_timestamp;
    delete row.mz_diff;
    delete row.mz_progress;

    return row;
}

function process(row, key_field) {
    /**
     * 1 or -1 values happens only when rows are unique.
     * - Greater values than 1 or lower values than -1 happens ONLY when the row is not unique
     */
    const count = parseInt(row.mz_diff);
    const _delete = count === -1;
    const key = row[key_field];
    const update = { key, value: clean(row), delete: _delete };

    return update;
}

async function createGlobalState(client) {
    await client.query("CREATE TABLE IF NOT EXISTS CLIENT_TEST_SAFE_TO_DROP (id INT, name TEXT);");
    await client.query("INSERT INTO CLIENT_TEST_SAFE_TO_DROP VALUES (12, 'board');");
}

async function updateGlobalState(client) {
    setTimeout(async () => {
        await client.query("DELETE FROM CLIENT_TEST_SAFE_TO_DROP;");
    }, 2000);
}

async function subscribe(client, query, headers) {
    const batch = [];
    const state = new State();
    const stream = client.query(copyTo(`COPY (SUBSCRIBE (${query}) WITH (PROGRESS = true)) TO STDOUT WITH (HEADER = true)`));

    csv({
        delimiter: "\t",
        headers: ['mz_timestamp','mz_progress', 'mz_diff', ...headers]
    })
    .fromStream(stream)
    .subscribe((row)=> {
        const { mz_progress } = row;

        if (mz_progress === "t") {
            state.batchUpdate([...batch]);
            batch.splice(0, batch.length);
        } else {
            const data = process(row);
            batch.push(data);
        }
    }, console.error, console.log);

    const stop = () => {
        const promise = new Promise ((res, rej) => {
            stream.on('close', res);
            stream.on('error', rej);
        });

        stream.destroy();

        return promise;
    }

    return { state, stop };
}

///
/// Integration test
///
describe("State integration test with Materialize", () => {
    it("Should subscribe to Materialize views", async () => {
        const pool = new Pool(config);

        const client = await pool.connect();
        // await client.query("SET CLUSTER = d;");

        try {
            await createGlobalState(client);

            const subscribeClient = await pool.connect();
            // await subscribeClient.query("SET CLUSTER = d;");

            try {
                const { state, stop } = await subscribe(subscribeClient, "SELECT id, name FROM CLIENT_TEST_SAFE_TO_DROP", ['id', 'name']);
                const internalState = state.getState();

                // Mutate the global state
                await new Promise(async (res,) => setTimeout(res, 5000));
                assert.equal(Object.keys(internalState).length, 1);

                await updateGlobalState(client);
                await new Promise(async (res,) => setTimeout(res, 5000));

                await stop();
                assert.equal(Object.keys(internalState).length, 0);
            } catch (err) {
                console.error(err);
            } finally {
                subscribeClient.release();
            }
        } catch (err) {
            console.error("Error running query: ", err);
        } finally {
            await client.query("DROP TABLE IF EXISTS CLIENT_TEST_SAFE_TO_DROP;");
            client.release();
            await pool.end();
        }
    }).timeout(20000);
});
