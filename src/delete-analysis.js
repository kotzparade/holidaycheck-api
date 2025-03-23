import { BigQuery } from "@google-cloud/bigquery";
import dotenv from "dotenv";
import config from "./config/config.js";

dotenv.config();

async function deleteAnalysisRows() {
  const bigquery = new BigQuery({
    projectId: config.projectId,
    keyFilename: config.keyFilename,
  });

  try {
    // Get all tables in the dataset
    const [tables] = await bigquery.dataset(config.datasetId).getTables();

    // Filter for analysis tables
    const analysisTables = tables.filter((table) =>
      table.id.endsWith("_analysis")
    );

    console.log(`Found ${analysisTables.length} analysis tables to clean`);

    // Delete rows from each analysis table
    for (const table of analysisTables) {
      const query = `DELETE FROM \`${config.projectId}.${config.datasetId}.${table.id}\` WHERE 1=1`;

      console.log(`Deleting rows from ${table.id}...`);

      await bigquery.query({
        query,
        location: "US",
      });

      console.log(`Successfully deleted rows from ${table.id}`);
    }

    console.log("All analysis tables have been cleaned");
  } catch (error) {
    console.error("Error deleting analysis rows:", error);
  }
}

// Run the deletion
deleteAnalysisRows();
