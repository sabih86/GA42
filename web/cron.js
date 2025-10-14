import cron from 'cron';
import https from 'https';

const backendUrl = 'https://ga4-3iq2.onrender.com/';

export const job = new cron.CronJob('*/14 * * * *', function () {
  // This function will be executed every 14 minutes.
  console.log('Restarting server');

  // Perform an HTTPS GET request to hit the backend API.
  https
    .get(backendUrl, (res) => {
      if (res.statusCode === 200) {
        console.log('✅ Server restarted successfully');
      } else {
        console.error(`❌ Failed to restart server — status code: ${res.statusCode}`);
      }
    })
    .on('error', (err) => {
      console.error('🚨 Error during restart:', err.message);
    });
});

// Start the cron job
job.start();
