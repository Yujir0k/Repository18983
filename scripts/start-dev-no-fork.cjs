const { startServer } = require("next/dist/server/lib/start-server")

const port = Number(process.env.PORT || "3000")

startServer({
  dir: process.cwd(),
  isDev: true,
  hostname: "0.0.0.0",
  port,
  allowRetry: false,
  minimalMode: false,
}).catch((error) => {
  console.error(error)
  process.exit(1)
})

