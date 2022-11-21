const State = require("../src/State");
const { strict: assert }  = require("node:assert");

describe("State test", () => {
  it("Should create a state", async () => {
    new State();
  });

  it("Should update the state", async () => {
    const state = new State();
    const key = "board";
    const value = { id: 1, name: key };
    const update = { key, value };
    const timestamp = new Date().getTime();

    state.update(update, timestamp);

    assert.deepEqual(state.get(key), value, "Invalid state.");
  });

  it("Should batch update the state", async () => {
    const state = new State();
    const value = { max: '4249158' };
    const updates = [
      { key: undefined, value: { max: '4248657' }, delete: false },
      { key: undefined, value, delete: false },
      { key: undefined, value: { max: '4248657' }, delete: true }
    ];
    const timestamp = new Date().getTime();

    state.batchUpdate(updates, timestamp);

    assert.deepEqual(state.get(JSON.stringify(value)), value, "Invalid state.");
  });

  it("Should update the state without a key", async () => {
    const state = new State();
    const value = { id: 1, name: "board" };
    const key = JSON.stringify(value);

    const update = { value };
    const timestamp = new Date().getTime();

    state.update(update, timestamp);

    assert.deepEqual(state.get([key]), value, "Invalid state.");
  });

  it("Should update the state with a delete", async () => {
    const state = new State();
    const key = "board";
    const value = { id: 1, name: key };
    const update = [{ key, value }];
    const delete_update = [{ key, value, delete: true }];
    const timestamp = new Date().getTime();

    state.update(update, timestamp);
    state.update(delete_update, timestamp);

    assert.deepEqual(state.get(key), undefined, "Error comparing state.");
  });

  it("Should have the correct last timestamp", async () => {
    const state = new State();
    const key = "board";
    const value = { id: 1, name: key };
    const updates = [{ key, value }];
    const timestamp = new Date().getTime();

    state.update(updates, timestamp);
    const stateTimestamp = state.getTimestamp();

    assert.equal(timestamp, stateTimestamp, "Error comparing timestamps.");
  });

  it("Should throw an error on a lagged update", async () => {
    const state = new State();
    const key = "board";
    const value = { id: 1, name: key };
    const update = [{ key, value }];
    const timestamp = new Date().getTime();

    state.update(update, timestamp);

    const lag_timestamp = timestamp - 10;
    let err;
    try {
      state.update(update, lag_timestamp);
    } catch (updateErr) {
      err = updateErr;
    }

    assert.notEqual(err, undefined);
  });
});
