import { BigQuery } from "@google-cloud/bigquery";
import { config } from "./config/index.js";

const bigquery = new BigQuery({
  projectId: config.projectId,
});

const reviewSchema = [
  { name: "review_id", type: "STRING" },
  { name: "title", type: "STRING" },
  { name: "general_text", type: "STRING" },
  { name: "rating_general", type: "FLOAT" },
  { name: "travel_date", type: "TIMESTAMP" },
  { name: "entry_date", type: "TIMESTAMP" },
  { name: "user_id", type: "STRING" },
  { name: "travel_reason", type: "STRING" },
  { name: "traveled_with", type: "STRING" },
  { name: "children", type: "INTEGER" },
  { name: "total_reviews", type: "INTEGER" },
];

async function setupBigQuery() {
  try {
    // Create dataset if it doesn't exist
    const [datasets] = await bigquery.getDatasets();
    const dataset = datasets.find((d) => d.id === config.datasetId);

    if (!dataset) {
      console.log(`Creating dataset ${config.datasetId}...`);
      await bigquery.createDataset(config.datasetId);
      console.log("Dataset created successfully");
    } else {
      console.log(`Dataset ${config.datasetId} already exists`);
    }

    // Create tables for each hotel
    for (const hotel of config.hotels) {
      const [tables] = await bigquery.dataset(config.datasetId).getTables();
      const table = tables.find((t) => t.id === hotel.tableId);

      if (!table) {
        console.log(
          `Creating table ${hotel.tableId} for hotel ${hotel.name}...`
        );
        await bigquery.dataset(config.datasetId).createTable(hotel.tableId, {
          schema: reviewSchema,
          description: `Reviews for hotel ${hotel.name} (ID: ${hotel.id})`,
        });
        console.log(`Table ${hotel.tableId} created successfully`);
      } else {
        console.log(
          `Table ${hotel.tableId} for hotel ${hotel.name} already exists`
        );

        // Check if total_reviews column exists
        const [tableMetadata] = await bigquery
          .dataset(config.datasetId)
          .table(hotel.tableId)
          .getMetadata();

        const hasTotalReviews = tableMetadata.schema.fields.some(
          (field) => field.name === "total_reviews"
        );

        if (!hasTotalReviews) {
          console.log(`Adding total_reviews column to ${hotel.tableId}...`);
          await bigquery.query({
            query: `
              ALTER TABLE \`${config.projectId}.${config.datasetId}.${hotel.tableId}\`
              ADD COLUMN total_reviews INTEGER
            `,
            location: "US",
          });
          console.log(`Added total_reviews column to ${hotel.tableId}`);
        }
      }
    }

    console.log("\nBigQuery setup completed successfully");
  } catch (error) {
    console.error("Error setting up BigQuery:", error);
    process.exit(1);
  }
}

setupBigQuery();
