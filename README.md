# HolidayCheck API Integration

This project integrates with the HolidayCheck API to fetch and analyze hotel reviews. It stores the data in BigQuery and provides analysis of the reviews using OpenAI's GPT models.

## Features

- Fetches hotel reviews from HolidayCheck API
- Stores reviews in BigQuery
- Analyzes reviews using OpenAI's GPT models
- Provides daily updates of reviews and total counts
- Monthly analysis of recent reviews
- Manual analysis capability for specific hotels or time periods

## Setup

1. Install dependencies:

```bash
npm install
```

2. Create a `.env` file with your API keys:

```env
HOLIDAYCHECK_API_KEY=your_api_key
OPENAI_API_KEY=your_openai_key
```

3. Set up BigQuery tables:

```bash
npm run setup
```

4. Set up analysis tables:

```bash
npm run setup-analysis
```

## Usage

### Daily Updates

Run daily updates to fetch new reviews and update total counts:

```bash
npm run daily-update
```

### Review Analysis

The system performs review analysis in two ways:

1. **Automatic Monthly Analysis**

   - Runs on the first day of each month
   - Analyzes reviews from the last 30 days
   - Part of the daily update process
   - Results are stored in hotel-specific analysis tables

2. **Manual Analysis**
   You can manually trigger analysis for:
   - All hotels: `npm run analyze`
   - Specific hotel: `npm run analyze "Hotel Name"`
   - Custom time period: `npm run analyze "" 60` (60 days)
   - Specific hotel and time: `npm run analyze "Hotel Name" 60`

### Analysis Process

The review analysis:

1. Fetches recent reviews from BigQuery
2. Processes reviews in batches of 10
3. Uses OpenAI to analyze:
   - Overall sentiment
   - Positive points
   - Negative points
   - Common themes
   - Areas for improvement
4. Stores results in German language
5. Formats results for easy reading in Looker Studio

### Data Structure

- Reviews are stored in hotel-specific tables
- Total review counts are stored in the `ReviewTotals` table
- Analysis results are stored in hotel-specific analysis tables

## Configuration

Edit `config/hotels.js` to add or modify hotels:

```javascript
export const hotels = [
  {
    id: "hotel-id",
    name: "Hotel Name",
    partnerId: "partner-id",
  },
];
```

## Maintenance

### Cleanup

To remove old total review rows:

```bash
npm run cleanup-totals
```

### Reset Analysis

To delete all analysis results:

```bash
npm run delete-analysis
```

## Error Handling

The system includes comprehensive error handling:

- Retries failed API requests
- Logs errors with detailed information
- Continues processing other hotels if one fails
- Validates OpenAI responses
- Handles rate limits and timeouts

## Logging

All operations are logged with:

- Timestamp
- Hotel name
- Operation type
- Success/failure status
- Error details when applicable
