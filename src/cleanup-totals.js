import { BigQuery } from "@google-cloud/bigquery";
import { config } from "./config/index.js";
import { hotels } from "./config/hotels.js";

async function cleanupTotalRows() {
  const bigquery = new BigQuery({
    projectId: config.projectId,
    keyFilename: config.keyFilename,
  });

  console.log("Starting cleanup of total_reviews rows...");

  for (const hotel of hotels) {
    try {
      console.log(`\nProcessing hotel: ${hotel.name}`);

      // Delete rows where review_id starts with 'total_count_'
      const query = `
        DELETE FROM \`${config.projectId}.${config.datasetId}.${hotel.tableId}\`
        WHERE review_id LIKE 'total_count_%'
      `;

      const [job] = await bigquery.query({
        query,
        location: "US",
      });

      console.log(`[${hotel.name}] Deleted total_reviews rows`);
    } catch (error) {
      console.error(`Error cleaning up ${hotel.name}:`, error);
    }
  }

  console.log("\nCleanup completed");
}

// Run the cleanup
cleanupTotalRows().catch(console.error);
