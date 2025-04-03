import { BigQuery } from "@google-cloud/bigquery";
import { config } from "./config/index.js";
import { hotels } from "./config/hotels.js";

async function deleteAnalysisRows() {
  console.log("Starting deletion of analysis rows...");

  const bigquery = new BigQuery({
    projectId: config.projectId,
    keyFilename: config.keyFilename,
  });

  for (const hotel of hotels) {
    try {
      console.log(`Processing hotel: ${hotel.name}`);

      // Delete all rows from the analysis table
      const [job] = await bigquery.query({
        query: `
          DELETE FROM \`${config.projectId}.${config.datasetId}.${hotel.tableId}_analysis\`
          WHERE 1=1
        `,
      });

      console.log(`[${hotel.name}] Deleted all analysis rows`);
    } catch (error) {
      console.error(`Error processing hotel ${hotel.name}:`, error);
    }
  }

  console.log("Deletion of analysis rows completed");
}

// Run the deletion
deleteAnalysisRows().catch(console.error);
