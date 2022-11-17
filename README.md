# streaming-state

Simple and agnostic streaming state handler for Node.js.

# Documentation

## Install

```bash
npm install streaming-state
```

## Getting started

Create the state:

```javascript
    const state = new State();
```

Update the state:

```javascript
    const key = "board";
    const value = { id: 1, name: key };
    const timestamp = new Date().getTime();

    const update = { key, value };

    state.update(update, timestamp);
```

Invalid state:

```javascript
    const key = "board";
    const value = { id: 1, name: key };
    const update = { key, value };

    const first_ts = new Date().getTime();
    const second_ts = new Date().getTime();

    state.update(update, second_ts);

    // Throws an error (invalid state)
    state.update(update, first_ts);
```

TBD:

* Handle sorts
* Immutable
* Hooks
