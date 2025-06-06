const pino = require('pino');

const loggerOptions = {
  ...(process.env.NODE_ENV === 'development'
    ? {
        transport: {
          target: 'pino-pretty',
          options: {
            colorize: true,
          },
        },
      }
    : { messageKey: 'message' }),
  formatters: {
    bindings() {
      return {};
    },
    level(level) {
      return { level };
    },
  },
};

const logger = pino(loggerOptions);

module.exports = logger;
