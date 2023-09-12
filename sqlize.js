const fs = require("fs");
const csv = require("csv-parser");
const path = require("path");
const yargs = require("yargs");
const moment = require("moment");

const argv = yargs
  .option("input", {
    alias: "i",
    describe: "Input CSV file path",
    demandOption: true,
    type: "string",
  })
  .option("delimiter", {
    alias: "d",
    describe: "Delimiter used in the CSV file",
    demandOption: true,
    type: "string",
  })
  .option("addPrimaryKey", {
    describe: "Add an incrementing primary key to the table schema",
    type: "boolean",
    default: true,
  })
  .option("dropTable", {
    describe: "Include a drop table statement",
    type: "boolean",
    default: false, // Default to not include the drop table statement
  })
  .help().argv;

if (argv.help) {
  yargs.showHelp();
  process.exit(0);
}

const inputFile = argv.input;
const outputFileName = `out/${path.basename(
  inputFile,
  path.extname(inputFile)
)}.sql`;
const outputFile = path.join(__dirname, outputFileName);

fs.writeFileSync(outputFile, "");

const tableName = path.basename(inputFile, path.extname(inputFile)); // Extract table name from file name

let tableSchema = {};

const rows = [];
let rowCount = 0;
let primaryKeyCounter = 1;

fs.createReadStream(inputFile)
  .pipe(csv({ separator: argv.delimiter }))
  .on("data", (row) => {
    if (rowCount++ < 200) {
      analyzeRow(row);
    }
    generateInsertStatement(row);
  })
  .on("end", () => {
    const dropTableStatement = generateDropTableStatement();
    const createTableStatement = generateCreateTableStatement();
    fs.appendFileSync(outputFile, dropTableStatement);
    fs.appendFileSync(outputFile, createTableStatement);
    fs.appendFileSync(outputFile, rows.join(""));
    console.log(
      `SQL statements generated successfully. Output file: ${outputFileName}`
    );
  });

function analyzeRow(row) {
  for (const [key, value] of Object.entries(row)) {
    if (!tableSchema[key]) {
      // Initialize column data type based on the first encountered value
      tableSchema[key] = inferDataType(value);
    } else {
      // If the column data type has already been set, check for a more specific data type
      const currentType = tableSchema[key];
      const newType = inferDataType(value);
      if (isMoreSpecificDataType(currentType, newType)) {
        tableSchema[key] = newType;
      }
    }
  }
}

function inferDataType(value) {
  if (/^-?\d+(\.\d+)?$/.test(value)) {
    if (Number.isInteger(Number(value))) {
      return "int";
    }
    return "decimal";
  } else if (isDatetime(value)) {
    return "datetime";
  } else if (/^\d{4}-\d{2}-\d{2}/.test(value)) {
    return "date";
  } else {
    return "text"; // Default to text
  }
}

function isDatetime(value) {
  return (
    moment(value, "YYYY-MM-DD HH:mm:ss.SSS", true).isValid() ||
    moment(value, "YYYY/MM/DD HH:mm:ss.SSS", true).isValid()
  );
}

function isMoreSpecificDataType(currentType, newType) {
  // Compare data types and return true if the new type is more specific
  const dataTypePriority = ["int", "decimal", "date", "text"];
  return (
    dataTypePriority.indexOf(newType) > dataTypePriority.indexOf(currentType)
  );
}

function generateCreateTableStatement() {
  const columns = Object.entries(tableSchema)
    .map(([columnName, dataType]) => `${columnName} ${dataType}`)
    .join(",\n  ");

  const primaryKey = argv.addPrimaryKey
    ? `id INT AUTO_INCREMENT PRIMARY KEY,`
    : "";

  return `CREATE TABLE IF NOT EXISTS ${tableName} (
  ${primaryKey}
  ${columns}
  );\n`;
}

function generateDropTableStatement() {
  return argv.dropTable ? `DROP TABLE IF EXISTS ${tableName};\n` : "";
}

function generateInsertStatement(row) {
  const columns = Object.keys(row).join(", ");
  const values = Object.values(row)
    .map((value) => `'${escapeSingleQuotes(value)}'`)
    .join(", ");

  const primaryKeyValue = argv.addPrimaryKey ? primaryKeyCounter++ : "";
  const insertStatement = `INSERT INTO ${tableName} (id, ${columns}) VALUES (${primaryKeyValue}, ${values});\n`;

  rows.push(insertStatement);
}

function escapeSingleQuotes(value) {
  return value.replace(/'/g, "''");
}
