// Extend the JSON interface to include rawJSON, available in Node.js v21.0.0+.
// More info: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON/rawJSON
declare global {
  interface JSON {
    rawJSON(value: unknown): unknown;
  }
}
export {};
