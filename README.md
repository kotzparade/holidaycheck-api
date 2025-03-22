# HolidayCheck API Integration

This Node.js application fetches hotel reviews from the HolidayCheck API and stores them in Google BigQuery.

## Prerequisites

1. Node.js 16 or higher
2. Google Cloud Project with BigQuery enabled
3. Google Cloud credentials set up
4. HolidayCheck API Partner ID

## Setup

1. Install dependencies:

```bash
npm install
```

2. Set up your environment variables by creating a `.env` file with the following variables:

```
PROJECT_ID=your-google-cloud-project-id
DATASET_ID=HolidaycheckHotelReviews
```

3. Set up Google Cloud credentials by either:

   - Setting the GOOGLE_APPLICATION_CREDENTIALS environment variable to point to your service account key file
   - Or using Google Cloud SDK authentication (`gcloud auth application-default login`)

4. Initialize the BigQuery dataset and tables:

```bash
npm run setup
```

## Adding a New Hotel

1. Open `src/config/hotels.js`
2. Add a new hotel configuration to the `hotels` array:

```javascript
{
  id: "your-hotel-id",              // HolidayCheck hotel ID
  name: "YourHotelName",           // A unique name for the hotel (used for table naming)
  partnerId: "your-partner-id",    // Your HolidayCheck partner ID
  tableId: "Reviews_YourHotelName" // Table name in BigQuery (must be unique)
}
```

Example:

```javascript
export const hotels = [
  {
    id: "291dac76-4fe2-3336-9c5d-709261abf797",
    name: "HotelOne",
    partnerId: "1798",
    tableId: "Reviews_HotelOne",
  },
  {
    id: "new-hotel-uuid",
    name: "HotelTwo",
    partnerId: "1798",
    tableId: "Reviews_HotelTwo",
  },
];
```

3. After adding the hotel configuration, run the setup script to create the new table:

```bash
npm run setup
```

## Usage

To fetch reviews and store them in BigQuery:

```bash
npm run daily-update
```

This will:

- Process each hotel independently
- Create separate tables for each hotel if they don't exist
- Fetch only new reviews that aren't already in the database
- Provide a summary of the import process for each hotel

## Features

- Fetches all reviews for configured hotels from HolidayCheck API
- Handles pagination automatically
- Implements rate limiting protection
- Validates review data before insertion
- Stores reviews in separate BigQuery tables per hotel
- Error handling and logging
- Daily import summary with success/failure status per hotel
