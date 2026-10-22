// Vercel build step. On production deploys, push the Convex backend (convex/) to the
// Convex production deployment using CONVEX_DEPLOY_KEY. Preview deploys skip it so a
// test branch can never change the live database. The site itself needs no build.
import { execSync } from "node:child_process";

if (process.env.VERCEL_ENV === "production") {
  if (!process.env.CONVEX_DEPLOY_KEY) {
    console.error("CONVEX_DEPLOY_KEY is not set in Vercel; cannot deploy the Convex backend.");
    process.exit(1);
  }
  execSync("npx convex deploy -y", { stdio: "inherit" });
} else {
  console.log("Not a production deploy (" + (process.env.VERCEL_ENV || "local") + "): skipping Convex deploy.");
}
