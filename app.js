// require modules
const dotenv = require('dotenv');
const path = require('path');
dotenv.config({ path: path.join(__dirname, '.env') });

const logger = require('./lib/logger');

const { MONGO_URI } = process.env;

const packageJson = require('./package.json');
process.env.VERSION = packageJson.version;

const mongoose = require('mongoose');
mongoose.set('strictQuery', false);

// mongoose connection
mongoose
  .connect(
    MONGO_URI,
    {
      useNewUrlParser: true,
      useUnifiedTopology: true
    }
  )
  .then(() => {
    logger.info('[DB][Mongo]: Connected');
    require('./models/Domain');

    // worker setup
    require('./worker2').default();
  });

process.env.instance = 'app';

// server setup
require('./server');
