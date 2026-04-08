const { startServer } = require("next/dist/server/lib/start-server")

function parsePortFromArgs(argv) {
  let positionalPort = null

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    if (!arg) continue

    if (arg === "--port" || arg === "-p") {
      const next = argv[i + 1]
      if (next && /^\d+$/.test(next)) return Number(next)
      continue
    }

    if (arg.startsWith("--port=")) {
      const value = arg.split("=")[1]
      if (value && /^\d+$/.test(value)) return Number(value)
      continue
    }

    if (/^\d+$/.test(arg) && positionalPort === null) {
      positionalPort = Number(arg)
    }
  }

  return positionalPort
}

function resolvePort() {
  const fromArgs = parsePortFromArgs(process.argv.slice(2))
  const fromEnv = /^\d+$/.test(String(process.env.PORT || ""))
    ? Number(process.env.PORT)
    : null

  const candidate = fromArgs ?? fromEnv ?? 3000
  if (!Number.isInteger(candidate) || candidate < 1 || candidate > 65535) {
    return 3000
  }
  return candidate
}

const port = resolvePort()

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
