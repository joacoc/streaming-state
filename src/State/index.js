/**
 * Simple streaming state handler for Node.JS made with love by J.
 */
module.exports = class State {
  #state;
  #stateCount;
  #timestamp;
  #valid;

  constructor(params) {
    const { state, stateCount, timestamp } = params || {};
    this.#state = state || {};
    this.#stateCount = stateCount || {};
    this.#timestamp = timestamp || 0;
    this.#valid = true;
  }

  get(key) {
    return this.#state[key];
  }

  keys() {
    return Object.keys(this.#state);
  }

  values() {
    return Object.values(this.#state);
  }

  isValid() {
    return this.#valid;
  }

  getTimestamp() {
    return this.#timestamp;
  }

  #increase(key) {
    // Count value starts as a NaN
    this.#stateCount[key] = this.#stateCount[key] + 1 || 1;
  }

  #decrease(key) {
    this.#stateCount[key] = this.#stateCount[key] - 1 || -1;
  }

  #hash(value) {
    return JSON.stringify(value);
  }

  #validate(timestamp) {
    if (!this.#valid) {
      throw new Error("Invalid state.");
    } else if (timestamp < this.#timestamp) {
      this.#valid = false;
      throw new Error(`Update with timestamp (${timestamp}) is lower than the last timestamp (${this.#timestamp}). Invalid state.`);
    }
  }

  #process({ key, value, delete: _delete }) {
    const _key = key || this.#hash(value);
    if (_delete) {
      this.#decrease(_key, 1);

      const count = this.#stateCount[_key];
      if (count <= 0) {
        delete this.#state[_key];
        delete this.#stateCount[_key];
      }
    } else {
      this.#increase(_key, 1);

      this.#state[_key] = value;
    }
  }

  update(update, timestamp) {
    this.#validate(timestamp);
    this.#timestamp = timestamp;
    this.#process(update)
  }

  batchUpdate(updates, timestamp) {
    if (Array.isArray(updates) && updates.length > 0) {
      this.#validate(timestamp);
      this.#timestamp = timestamp;
      updates.forEach(this.#process.bind(this));
    }
  }
};
