import dotenv from "dotenv";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { hotels } from "./hotels.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config({ path: join(__dirname, "../../.env") });

export const config = {
  projectId: process.env.PROJECT_ID,
  datasetId: process.env.DATASET_ID,
  hotels: hotels,
  api: {
    baseUrl: "https://www.holidaycheck.de/svc/api-hotelreview/v3",
    defaultLocale: "de",
    batchSize: 50,
    maxRetries: 3,
    initialRetryDelay: 1000,
    maxOffset: 1000,
  },
};
