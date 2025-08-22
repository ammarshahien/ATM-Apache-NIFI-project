# ATM NiFi Project

This repository contains an Apache NiFi flow for processing ATM transactions. It handles withdrawals, deposits, error logging, and integration with MongoDB for storage.

## Overview
- **Flow Description**: The flow receives ATM transactions via HTTP, routes them based on type (withdrawal/deposit/failed), processes them (e.g., flagging high-value transactions, adding metadata), stores in MongoDB, and cleans up files.
- **Components**:
  - Process Groups: ATM Transaction Generator, Withdrawal Processing, Deposit Processing, Failed Transaction Handler, Delete Flow Files.
  - Key Processors: ListenHTTP (for receiving transactions), EvaluateJsonPath (extract fields), RouteOnAttribute (routing), PutMongo (store in DB), DeleteFile (cleanup).
  - Tools: MongoDB integration, logging, merging content.

## Prerequisites
- Apache NiFi (version 1.28.1 or compatible).
- MongoDB running on `localhost:27017` (or update the URI in PutMongo processors).
- Input directory: `C:\Users\DELL\Downloads\nifi-1.28.1-bin\nifi-1.28.1\nifi-data\` (update paths as needed).

## Setup Instructions
1. Download and install Apache NiFi from [nifi.apache.org](https://nifi.apache.org/).
2. Start NiFi: Run `bin/nifi.sh start` (Linux/Mac) or `bin\nifi.bat run` (Windows).
3. Access NiFi UI: Go to `https://localhost:8443/nifi` (default port; adjust if changed).
4. Import the Flow:
   - Drag the "Template" icon onto the canvas.
   - Select "Upload Template" and choose `ATM_project.json` from this repo.
   - Instantiate the template on the canvas.
5. Configure:
   - Update file paths, MongoDB URI, and ports in processors (e.g., GetFile, PutMongo, ListenHTTP).
   - Enable controller services (e.g., for MongoDB if needed).
6. Start the flow and test by sending JSON transactions to the ListenHTTP endpoint (e.g., via curl or Postman).

## Sample Input JSON
```json
{
  "transaction_id": "12345",
  "transaction_type": "WITHDRAWAL",
  "amount": 5000,
  "atm_id": "ATM_001",
  "customer_id": "CUST_001",
  "transaction_date": "2025-08-22T10:00:00Z",
  "status": "SUCCESS",
  "hour": 10,
  "region": "US"
}
