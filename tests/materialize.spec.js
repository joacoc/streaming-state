const State = require("../src/State");
const { strict: assert }  = require("node:assert");
const { Pool } = require("pg");
const { to: copyTo } = require('pg-copy-streams');
const csv = require("csvtojson");

const { env } = process;
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
        await client.query("SET CLUSTER = d;");

        try {
            await createGlobalState(client);

            const subscribeClient = await pool.connect();
            await subscribeClient.query("SET CLUSTER = d;");

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


// async function* asyncFetcher(client, cursorId) {
//     const queryParams = {
//       text: `FETCH 1000 ${cursorId} WITH (TIMEOUT='1');`,
//       values: [],
//     };

//     while (true) {
//       const results = await client.query(queryParams);
//       const { rows } = results;
//       const timestamp = new Date().getTime();

//       /**
//        * 1 or -1 values happens only when rows are unique.
//        * - Greater values than 1 or lower values than -1 happens ONLY when the row is not unique
//        */
//       for (const row of rows) {
//         const count = parseInt(row.mz_diff);
//         const _delete = count === -1;
//         const key = row.id;
//         const update = { key, value: clean(row), delete: _delete };

//         yield { update, timestamp } ;
//       }
//     }
// }

// async function processFetch({ rows }) {
//     const batches = [];
//     const batch = [];

//     for (const i of rows) {
//         const data = rows[i];

//         const { mz_progressed } = data;
//         if (mz_progressed) {
//             updatesPosition = i;
//         } else {
//             results.push(schema(clean(data)));
//         }
//     }

//     return {
//         results,
//     }
// }

// async function getGlobalState(client, cursor) {
//     const { rows } = await client.query(`FETCH ALL ${cursor};`);
//     const globalState = [];
//     const updates = [];
//     let updatesPosition;


//     globalState.push(rows.splice(updatesPosition, rows.length));

//     return { globalState, updates };
// }
//
// async function subscribe(client, query, headers) {
//     // const cursorId = "C";
//     // await client.query("BEGIN;");
//     // await client.query(`DECLARE ${cursorId} CURSOR FOR SUBSCRIBE (${query}) WITH (PROGRESS);`);

//     const { globalState, updates } = await getGlobalState(client, cursorId);
//     state.batchUpdate(globalState);


//     const runInfiniteCode = async () => {
//         // Extra tiring code
//         for await ({ update, timestamp } of asyncFetcher(client, cursorId)) {

//             console.log("Update: ", update);
//             state.update(update, timestamp);
//             // break;
//         };
//     };

//     runInfiniteCode();
//     await new Promise((res, rej) => {});

//     return state;
// }
