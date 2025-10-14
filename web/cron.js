const cron = require('cron');
const https = require('https');

const backendUrl = 'https://ga4-3iq2.onrender.com/';

const job = new cron.CronJob('*/14 * * * *', function () {
  // This function will be executed every 14 minutes.
  console.log('Restarting server');

  // Perform an HTTPS GET request to hit the backend API.
  https.get(backendUrl, (res) => {
    if (res.statusCode === 200) {
      console.log('Server restarted');
    } else {
      console.error(`Failed to restart server with status code: ${res.statusCode}`);
    }
  }).on('error', (err) => {
    console.error('Error during Restart:', err.message);
  });
});

// Start the cron job.
job.start();

// Export the cron job.
module.exports = {
  job,
};