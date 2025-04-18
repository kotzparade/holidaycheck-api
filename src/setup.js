import { BigQuery } from "@google-cloud/bigquery";
import { config } from "./config/index.js";
import { hotels } from "./config/hotels.js";

const reviewSchema = [
  { name: "review_id", type: "STRING", mode: "REQUIRED" },
  { name: "title", type: "STRING", mode: "REQUIRED" },
  { name: "general_text", type: "STRING", mode: "REQUIRED" },
  { name: "rating_general", type: "FLOAT64", mode: "NULLABLE" },
  { name: "travel_date", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "entry_date", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "user_id", type: "STRING", mode: "REQUIRED" },
  { name: "travel_reason", type: "STRING", mode: "NULLABLE" },
  { name: "traveled_with", type: "STRING", mode: "NULLABLE" },
  { name: "children", type: "INTEGER", mode: "REQUIRED" },
];

const totalSchema = [
  { name: "hotel_id", type: "STRING", mode: "REQUIRED" },
  { name: "hotel_name", type: "STRING", mode: "REQUIRED" },
  { name: "total_reviews", type: "INTEGER", mode: "REQUIRED" },
  { name: "last_updated", type: "TIMESTAMP", mode: "REQUIRED" },
];

async function setupBigQuery() {
  const bigquery = new BigQuery({
    projectId: config.projectId,
    keyFilename: config.keyFilename,
  });

  try {
    // Create dataset if it doesn't exist
    const [dataset] = await bigquery
      .createDataset(config.datasetId, { location: "US" })
      .catch(() => bigquery.dataset(config.datasetId).get());

    console.log(`Dataset ${config.datasetId} is ready`);

    // Create ReviewTotals table
    const [totalsTable] = await bigquery
      .dataset(config.datasetId)
      .createTable("ReviewTotals", { schema: totalSchema })
      .catch(() =>
        bigquery.dataset(config.datasetId).table("ReviewTotals").get()
      );

    console.log("ReviewTotals table is ready");

    // Create tables for each hotel
    for (const hotel of hotels) {
      const [table] = await bigquery
        .dataset(config.datasetId)
        .createTable(hotel.tableId, { schema: reviewSchema })
        .catch(() =>
          bigquery.dataset(config.datasetId).table(hotel.tableId).get()
        );

      console.log(`Table for ${hotel.name} is ready`);
    }

    console.log("\nAll tables are ready!");
  } catch (error) {
    console.error("Error setting up BigQuery:", error);
    throw error;
  }
}

// Run the setup
setupBigQuery().catch(console.error);
