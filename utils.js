const logger = require("./lib/logger");

const disallowedValues = [
  '[not provided]',
  'placeholder',
  '[[unknown]]',
  'not set',
  'not provided',
  'unknown',
  'undefined',
  'n/a'
];

const filterNullValuesFromObject = object =>
  Object
    .fromEntries(
      Object
        .entries(object)
        // eslint-disable-next-line no-unused-vars
        .filter(([_, v]) =>
          v !== null &&
          v !== '' &&
          typeof v !== 'undefined' &&
          (typeof v !== 'string' || !disallowedValues.includes(v.toLowerCase()) || !v.toLowerCase().includes('!$record'))));

const normalizePropertyName = key => key.toLowerCase().replace(/__c$/, '').replace(/^_+|_+$/g, '').replace(/_+/g, '_');

const goal = actions => {
  // this is where the data will be written to the database
  logger.info({
    tail: actions.at(-1),
    totalActions: actions.length,
    // TODO: Add LOG_PREFIX to cron config
  }, `[DB][Save]: ${actions.length} actions saved.`);
};

module.exports = {
  filterNullValuesFromObject,
  normalizePropertyName,
  goal
};
