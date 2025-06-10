import delay from "./delay";

const retry = async (cb, retryCount, delayInMs) => {
  let result;
  let err;

  let currIdx = 0;

  while (currIdx < retryCount) {
    try {
      result = await cb();

      break;
    } catch (error) {
      err = error;
    }

    await delay(delayInMs);

    currIdx += 1;
  }

  if (result) return result;

  throw err;
};

export default retry;