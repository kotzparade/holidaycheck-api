import { BigQuery } from "@google-cloud/bigquery";
import { config } from "./config/index.js";

const bigquery = new BigQuery({
  projectId: config.projectId,
});

async function migrateData() {
  console.log("Starting data migration...");

  try {
    // First, verify the old table exists
    const [tables] = await bigquery.dataset(config.datasetId).getTables();
    const oldTable = tables.find((t) => t.id === "Reviews");

    if (!oldTable) {
      console.log('Old "Reviews" table not found. No migration needed.');
      return;
    }

    // Get the first hotel from config (assuming this is where we want to migrate the data)
    const targetHotel = config.hotels[0];
    if (!targetHotel) {
      console.error(
        "No target hotel configured. Please check src/config/hotels.js"
      );
      process.exit(1);
    }

    console.log(`Migrating data to ${targetHotel.tableId}...`);

    // Copy data from old table to new table
    const query = `
      INSERT INTO \`${config.projectId}.${config.datasetId}.${targetHotel.tableId}\`
      SELECT * FROM \`${config.projectId}.${config.datasetId}.Reviews\`
      WHERE NOT EXISTS (
        SELECT 1 
        FROM \`${config.projectId}.${config.datasetId}.${targetHotel.tableId}\` target 
        WHERE target.review_id = Reviews.review_id
      )
    `;

    const [job] = await bigquery.createQueryJob({
      query,
      location: "US",
    });

    await job.getQueryResults();

    // Get count of migrated rows
    const [countRows] = await bigquery.query({
      query: `
        SELECT COUNT(*) as count 
        FROM \`${config.projectId}.${config.datasetId}.${targetHotel.tableId}\`
      `,
      location: "US",
    });

    console.log(
      `Successfully migrated data. New table has ${countRows[0].count} reviews.`
    );

    // Ask user if they want to delete the old table
    console.log(
      "\nIMPORTANT: Please verify that the data has been migrated correctly."
    );
    console.log(
      'To delete the old "Reviews" table, run this command in BigQuery:'
    );
    console.log(
      `DROP TABLE \`${config.projectId}.${config.datasetId}.Reviews\``
    );
  } catch (error) {
    console.error("Error during migration:", error);
    process.exit(1);
  }
}

migrateData();
