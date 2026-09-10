const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);
const input = JSON.parse(Buffer.concat(chunks).toString("utf8"));
console.log(JSON.stringify({
  outcome: "completed",
  message: "Example process completed",
  output: { input },
}));
