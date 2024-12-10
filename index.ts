import { Pool } from "pg"; // PostgreSQL client
import Client, {
  CommitmentLevel,
  SubscribeRequestAccountsDataSlice,
  SubscribeRequestFilterAccounts,
  SubscribeRequestFilterBlocks,
  SubscribeRequestFilterBlocksMeta,
  SubscribeRequestFilterEntry,
  SubscribeRequestFilterSlots,
  SubscribeRequestFilterTransactions,
} from "@triton-one/yellowstone-grpc";
import { SubscribeRequestPing } from "@triton-one/yellowstone-grpc/dist/grpc/geyser";
import { Connection, PublicKey } from "@solana/web3.js";
import { tOutPut } from "./utils/transactionOutput";
import { getTokenInfo } from "./utils/tokenInfo";
import { getTokenBalance } from "./utils/token";

const pumpfun = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const api = "api";
const connection = new Connection(`https://rpc.shyft.to?api_key=${api}`, "confirmed");
let activeStream: any; // Reference to the active stream

// PostgreSQL connection pool
const pool = new Pool({
  user: "your_user",
  host: "your_host",
  database: "your_database",
  password: "your_password",
  port: 5432,
});

interface SubscribeRequest {
  accounts: { [key: string]: SubscribeRequestFilterAccounts };
  slots: { [key: string]: SubscribeRequestFilterSlots };
  transactions: { [key: string]: SubscribeRequestFilterTransactions };
  transactionsStatus: { [key: string]: SubscribeRequestFilterTransactions };
  blocks: { [key: string]: SubscribeRequestFilterBlocks };
  blocksMeta: { [key: string]: SubscribeRequestFilterBlocksMeta };
  entry: { [key: string]: SubscribeRequestFilterEntry };
  commitment?: CommitmentLevel | undefined;
  accountsDataSlice: SubscribeRequestAccountsDataSlice[];
  ping?: SubscribeRequestPing | undefined;
}

// Function to get the new token address from the PostgreSQL table
async function getNewTokenAddresses(): Promise<string[]> {
  try {
    const result = await pool.query("SELECT contractaddress FROM tokens WHERE active = true;");
    return result.rows.map((row) => row.contractaddress);
  } catch (error) {
    console.error("Error fetching token addresses from the database:", error);
    return [];
  }
}

async function handleStream(client: Client, args: SubscribeRequest) {
  // End the existing stream if it's active
  if (activeStream) {
    console.log("Stopping existing stream...");
    activeStream.end();
    activeStream = null;
  }

  // Start a new stream
  const stream = await client.subscribe();
  activeStream = stream; // Save the current stream

  // Handle stream events
  const streamClosed = new Promise<void>((resolve, reject) => {
    stream.on("error", (error) => {
      console.error("Stream error:", error);
      reject(error);
      stream.end();
    });
    stream.on("end", () => {
      console.log("Stream ended.");
      resolve();
    });
    stream.on("close", () => {
      console.log("Stream closed.");
      resolve();
    });
  });

  // Handle data from the stream
  stream.on("data", async (data) => {
    try {
      const result = await tOutPut(data);
      const bondingDetails = await getBondingCurveAddress(result.meta.postTokenBalances);
      const Ca = result.meta.postTokenBalances[0].mint;
      const tokenInfo = await getTokenInfo(Ca);
      const bondingCurve = bondingDetails.bondingCurve ? bondingDetails.bondingCurve.toString() : "";
      const tokenBalances = await getTokenBalance(bondingCurve);
      const PoolValue = bondingDetails.solBalance / 1000000000;
      const marketInfo = calculateInfo(PoolValue, tokenBalances, tokenInfo);

      if (tokenInfo) {
        console.log(`
          Latest Pool
          Ca : ${Ca}
          Bonding Curve Address : ${bondingCurve}
          Pool Value SOL : ${Number(PoolValue).toFixed(2)} SOL
          Pool Value : ${tokenBalances}
          Price : $${marketInfo.price}
          MarketCap : ${marketInfo.marketPrice}
          Current Supply : ${tokenInfo}
        `);
      }
    } catch (error) {
      console.error("Error processing stream data:", error);
    }
  });

  // Send subscribe request
  await new Promise<void>((resolve, reject) => {
    stream.write(args, (err: any) => {
      if (!err) {
        resolve();
      } else {
        reject(err);
      }
    });
  }).catch((reason) => {
    console.error("Failed to send subscribe request:", reason);
    throw reason;
  });

  await streamClosed;
}

// Function to periodically renew the stream every 10 seconds with new token addresses
async function startPeriodicStream(client: Client) {
  const interval = 5000; // 10 seconds
  while (true) {
    try {
      console.log("Fetching new token addresses...");
      const tokenAddresses = await getNewTokenAddresses();
      if (tokenAddresses.length > 0) {
        const req: SubscribeRequest = {
          accounts: {},
          slots: {},
          transactions: {
            pumpfun: {
              vote: false,
              failed: false,
              signature: undefined,
              accountInclude: tokenAddresses, // Use the new token addresses
              accountExclude: [],
              accountRequired: [],
            },
          },
          transactionsStatus: {},
          entry: {},
          blocks: {},
          blocksMeta: {},
          accountsDataSlice: [],
          ping: undefined,
          commitment: CommitmentLevel.CONFIRMED, // For receiving confirmed transaction updates
        };

        console.log("Starting new stream with updated token addresses...");
        await handleStream(client, req);
      } else {
        console.log("No active token addresses found.");
      }
    } catch (error) {
      console.error("Stream error, restarting in next interval...", error);
    }
    await new Promise((resolve) => setTimeout(resolve, interval)); // Wait for the interval
  }
}

const client = new Client("gRPC REGION URL", "gRPC TOKEN", undefined);

// Start periodic stream with database integration
startPeriodicStream(client);

async function getBondingCurveAddress(transaction: any[]) {
  let bondingCurve;
  let solBalance;
  const eachOwners = transaction?.flatMap((inner) => inner.owner);
  for (const owner in eachOwners) {
    const address = new PublicKey(eachOwners[owner]);
    const systemOwner = await connection.getAccountInfo(address);
    if (systemOwner?.owner.toString() === pumpfun) {
      bondingCurve = address;
      solBalance = systemOwner.lamports;
      return { bondingCurve, solBalance };
    }
  }
  return { bondingCurve, solBalance };
}

function calculateInfo(solBal: number, tokenBal: number, currentSupply: number) {
  const $sol: number = solBal * 134.7;
  const tokenBought: number = currentSupply - tokenBal;
  const tokenBoughtPrice: number = $sol / tokenBought;
  const tokenValue = tokenBoughtPrice * currentSupply;
  const price = tokenValue / tokenBal;
  const marketPrice = price * currentSupply;
  return {
    price,
    marketPrice,
  };
}
