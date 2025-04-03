import { BigQuery } from "@google-cloud/bigquery";
import { config } from "./config/index.js";
import { hotels } from "./config/hotels.js";

const analysisSchema = [
  { name: "analysis_date", type: "TIMESTAMP", mode: "REQUIRED" },
  { name: "days_analyzed", type: "INTEGER", mode: "REQUIRED" },
  { name: "overall_sentiment", type: "STRING", mode: "REQUIRED" },
  { name: "positive_points", type: "STRING", mode: "REQUIRED" },
  { name: "negative_points", type: "STRING", mode: "REQUIRED" },
  { name: "common_themes", type: "STRING", mode: "REQUIRED" },
  { name: "areas_for_improvement", type: "STRING", mode: "REQUIRED" },
  { name: "total_reviews", type: "INTEGER", mode: "REQUIRED" },
  { name: "analyzed_reviews", type: "INTEGER", mode: "REQUIRED" },
];

// New schema for tracking analyzed review IDs
const analyzedReviewIdsSchema = [
  { name: "review_id", type: "STRING", mode: "REQUIRED" },
  { name: "analysis_timestamp", type: "TIMESTAMP", mode: "REQUIRED" },
];

const totalSchema = [
  { name: "hotel_id", type: "STRING", mode: "REQUIRED" },
  { name: "hotel_name", type: "STRING", mode: "REQUIRED" },
  { name: "total_reviews", type: "INTEGER", mode: "REQUIRED" },
  { name: "last_updated", type: "TIMESTAMP", mode: "REQUIRED" },
];

async function setupAnalysisTables() {
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

    // Create analysis tables for each hotel
    for (const hotel of hotels) {
      const tableId = `${hotel.tableId}_analysis`;
      const [table] = await bigquery
        .dataset(config.datasetId)
        .createTable(tableId, { schema: analysisSchema })
        .catch(() => bigquery.dataset(config.datasetId).table(tableId).get());

      console.log(`Analysis table for ${hotel.name} is ready`);
    }

    // Create AnalyzedReviewIDs table (if it doesn't exist)
    const [analyzedIdsTable] = await bigquery
      .dataset(config.datasetId)
      .createTable("AnalyzedReviewIDs", { schema: analyzedReviewIdsSchema })
      .catch(() =>
        bigquery.dataset(config.datasetId).table("AnalyzedReviewIDs").get()
      );
    console.log("AnalyzedReviewIDs table is ready");

    console.log("\nAll analysis tables are ready!");
  } catch (error) {
    console.error("Error setting up analysis tables:", error);
    throw error;
  }
}

// Run the setup
setupAnalysisTables().catch(console.error);
