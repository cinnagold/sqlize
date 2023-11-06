const fs = require("fs");
const csv = require("csv-parser");
const path = require("path");
const yargs = require("yargs");
const moment = require("moment");
const { table } = require("console");

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
    default: ",",
  })
  .option("addPrimaryKey", {
    describe: "Add an incrementing primary key to the table schema",
    type: "boolean",
    default: true,
  })
  .option("primaryKeyColumn", {
    describe: "The name of the column in the file to use as the primary key",
    type: "string",
  })
  .option("addForeignKeyToFile", {
    describe:
      "Add a foreign key column for the default primary key being used in this csv file (note, the delimeter must match across both files)",
    type: "string",
  })
  .option("addForeignKeyColumn", {
    describe:
      "The column to use when adding a foreign key column. Note, the column names in both files must match for this foreign key column",
    type: "string",
  })
  .option("addBlankColumns", {
    describe: "A comma-separated list of blank columns to add",
    type: "string",
  })
  .option("forceTextColumns", {
    describe: "A comma-separated list of columns that should be treated as text columns regardless of their content",
    type: "string",
  })
  .option("dropTable", {
    describe: "Include a drop table statement",
    type: "boolean",
    default: true,
  })
  .option("lookup", {
    describe: "Specify a column for lookup table generation",
    type: "string",
    default: ""
  })
  .option("sqlbatchsize", {
    describe: "Number of rows to include with each SQL INSERT statement",
    type: "number",
    default: 1000,
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

const tableName = path
  .basename(inputFile, path.extname(inputFile))
  .toLowerCase();

let tableSchema = {};
const columnIndexToDataTypeMap = {};

const rows = [];
let primaryKeyCounter = 1;

let lookupTableName;
let lookupPkCounter = 1;
const lookupTableValues = {};

const lookupColumn = argv.lookup;
if (lookupColumn) {
  createLookupTable(lookupColumn);
}

const forceTextColumns= {};
if (argv.forceTextColumns) {
  argv.forceTextColumns
    .split(",")
    .map((column) => column.trim().toLowerCase())
    .forEach((column) => {
      forceTextColumns[column] = true;
    });
}

let foreignKeyvalues = {};
if (argv.addForeignKeyToFile || argv.addForeignKeyColumn) {
  computeForeignKeys(analyzeAndProcessCSVFile);
} else {
  analyzeAndProcessCSVFile();
}

function analyzeAndProcessCSVFile() {
  let rowCount = 0;
  const readStream = fs
    .createReadStream(inputFile)
    .pipe(csv({ separator: argv.delimiter }))
    .on("data", (row) => {
      if (rowCount++ < 5000) {
        analyzeRow(row);
      } else {
        readStream.destroy();
        setDefaultDataTypes();
        createColumnIndexToDataTypeMap();
        readCSVFile();
      }
    });
}

function setDefaultDataTypes() {
  for (const key in tableSchema) {
    if (tableSchema.hasOwnProperty(key) && tableSchema[key] === "null") {
      tableSchema[key] = "varchar(255)";
    }
  }
}

function createColumnIndexToDataTypeMap() {
  for (const key in tableSchema) {
    if (tableSchema.hasOwnProperty(key)) {
      const index = Object.keys(tableSchema).indexOf(key);
      columnIndexToDataTypeMap[index] = tableSchema[key];
    }
  }
}

function readCSVFile() {
  fs.createReadStream(inputFile)
    .pipe(csv({ separator: argv.delimiter }))
    .on("data", (row) => {
      generateInsertStatement(row);
    })
    .on("end", () => {
      if (lookupColumn) {
        const lookupTableInsertStatement =
          generateLookupTableInsertStatements();
      }
      const dropTableStatement = generateDropTableStatement(tableName);
      const createTableStatement = generateCreateTableStatement();
      fs.appendFileSync(outputFile, dropTableStatement);
      fs.appendFileSync(outputFile, createTableStatement);

      insertRows();
      console.log(
        `SQL statements generated successfully. Output file: ${outputFileName}`
      );
    });
}

function insertRows() {
  const columnsList = extractColumnsAsList(tableSchema),
    statements = [];

  for (let i = 0; i < rows.length; i += argv.sqlbatchsize) {
    const batchData = rows.slice(i, i + argv.sqlbatchsize);

    const sql = `INSERT INTO ${tableName} (${columnsList.join(
      ", "
    )})\n VALUES ${batchData.join(",\n")};`;
    statements.push(sql);
  }
  fs.appendFileSync(outputFile, statements.join("\n\n"));
}

function extractColumnsAsList(schema) {
  const list = [];
  Object.keys(schema).forEach((key) => {
    list.push(key);
  });

  return list;
}

function generateLookupTableInsertStatements() {
  const statements = [];
  Object.keys(lookupTableValues).forEach((key) => {
    const value = lookupTableValues[key];
    const insertStatement = `INSERT INTO ${lookupTableName} (id, value) VALUES (${value}, '${escapeSingleQuotes(
      key
    )}');`;
    statements.push(insertStatement);
  });

  fs.appendFileSync(outputFile, statements.join("\n"));
}

function analyzeRow(row) {
  for (let [key, value] of Object.entries(row)) {
    key = key.toLowerCase();
    if (key === lookupColumn) {
      tableSchema[key] = "int";
      continue;
    }

    if (forceTextColumns[key]) {
      tableSchema[key] = "varchar(255)";
      continue;
    }

    if (!tableSchema[key]) {
      // Initialize column data type based on the first encountered
      tableSchema[key] = inferDataType(value);
    } else {
      // If the column data type has already been set, check for a more specific data type
      const currentType = tableSchema[key];

      if (value != null && value !== "") {
        const newType = inferDataType(value);
        if (isMoreSpecificDataType(currentType, newType)) {
          tableSchema[key] = newType;
        }
      }
    }
  }
}

function inferDataType(value) {
  if (!value) {
    return "null";
  }

  if (/^-?\d+(\.\d+)?$/.test(value)) {
    if (Number.isInteger(Number(value))) {
      if (Number(value) > 2147483647) {
        //Max value for int in MariaDB
        return "varchar(100)";
      }
      if (value === "0.0") {
        return "decimal(15, 5)";
      }
      return "int";
    }
    return "decimal(15, 5)";
  } else if (isDatetime(value)) {
    return "datetime";
  } else if (/^\d{4}-\d{2}-\d{2}/.test(value)) {
    return "date";
  } else {
    return "varchar(255)";
  }
}

function isDatetime(value) {
  let isDateTime = moment(value, "M/D/YY HH:mm", true).isValid();


  return moment(
    value,
    [
      "YYYY-MM-DD HH:mm:ss.SSS",
      "DD-MM-YYYY HH:mm:ss.SSS",
      "YYYY-MM-DD HH:mm:ssZ",
      "YYYY-MM-DD HH:mm:ss",
      "YYYY/MM/DD HH:mm:ss.SSS",
      "M/D/YY H:mm"
    ],
    true
  ).isValid();
}

function isMoreSpecificDataType(currentType, newType) {
  // Compare data types and return true if the new type is more specific
  const dataTypePriority = [
    "null",
    "int",
    "decimal(15, 5)",
    "date",
    "datetime",
    "varchar(100)",
    "varchar(255)",
  ];
  return (
    dataTypePriority.indexOf(newType) > dataTypePriority.indexOf(currentType)
  );
}

function generateCreateTableStatement() {
  if (argv.addPrimaryKey) {
    tableSchema["id"] = "INT AUTO_INCREMENT PRIMARY KEY";
  }
  if (argv.addForeignKeyColumn) {
    tableSchema[`${argv.addForeignKeyColumn}_fk`] = "INT";
  }

  let columns = Object.entries(tableSchema)
  .map(([columnName, dataType]) => {
    const isPrimaryKey = columnName === argv.primaryKeyColumn;
    if (isPrimaryKey) {
      return `${columnName} ${dataType} PRIMARY KEY`;
    } else {
      return `${columnName} ${dataType}`;
    }
  })
    .join(",\n  ");

  if (argv.addBlankColumns) {
    const blankColumns = argv.addBlankColumns
      .split(",")
      .map((column) => column.trim());
    blankColumns.forEach((columnName) => {
      columns += `,\n${columnName.toLowerCase()} varchar(100)`;
    });
  }

  return `CREATE TABLE IF NOT EXISTS ${tableName} (
  ${columns}
  );\n`;
}

function generateDropTableStatement(table) {
  return argv.dropTable ? `\n\nDROP TABLE IF EXISTS ${table};\n` : "";
}

function generateInsertStatement(row) {
  const values = Object.values(row)
    .map((value, index) => {
      if (Object.keys(row)[index].toLowerCase() === argv.lookup.toLowerCase()) {
        const lookupValue = escapeSingleQuotes(value);
        if (!lookupTableValues[lookupValue]) {
          addLookupValue(lookupValue);
        }
        value = lookupTableValues[lookupValue];
      }

      if (columnIndexToDataTypeMap[index] === "datetime") {
        const parsedDate = moment(value, 'M/D/YY HH:mm');
        if (parsedDate.isValid()) {
          // Format the date in MariaDB compatible datetime format
          value = parsedDate.format('YYYY-MM-DD HH:mm:ss');
        }
      }

      return `${quoteIfNeeded(value, columnIndexToDataTypeMap[index])}`;
    })
    .join(", ");

  const primaryKeyValue = argv.addPrimaryKey ? primaryKeyCounter++ : "";
  const foreignKeyValue = argv.addForeignKeyColumn
    ? foreignKeyFor(row[argv.addForeignKeyColumn])
    : "";

  const insertParts = [values, primaryKeyValue, foreignKeyValue].filter(
    (part) => part !== ""
  );
  const insertStatement = `(${insertParts.join(", ")})`;
  rows.push(insertStatement);
}

function foreignKeyFor(value) {
  return foreignKeyvalues[value] || "NULL";
}

function addLookupValue(value) {
  lookupTableValues[value] = lookupPkCounter++;
}

function quoteIfNeeded(value, datatype) {
  const needQuotes = ["varchar(100)", "varchar(255)", "date", "datetime"];
  if (!value) {
    return `null`;
  }

  if (needQuotes.includes(datatype)) {
    return `'${escapeSingleQuotes(value)}'`;
  } else {
    return `${value}`;
  }
}

function escapeSingleQuotes(value) {
  if (typeof value === "string") {
    return value.replace(/'/g, "''");
  }
  return value;
}

function createLookupTable(columnName) {
  lookupTableName = `${tableName}_${columnName}_lookup`;

  const dropTableStatement = generateDropTableStatement(lookupTableName);
  fs.appendFileSync(outputFile, dropTableStatement);

  const lookupTableSchema = {
    id: "INT AUTO_INCREMENT PRIMARY KEY",
    value: "VARCHAR(100)",
  };

  const lookupTableCreateStatement = `CREATE TABLE IF NOT EXISTS ${lookupTableName} (
    ${Object.entries(lookupTableSchema)
      .map(([col, type]) => `${col} ${type}`)
      .join(",\n")}
  );\n`;

  fs.appendFileSync(outputFile, lookupTableCreateStatement);
}

function computeForeignKeys(andThen) {
  checkForeignKeyArgs();

  let pkCounter = 0;
  const readStream = fs
    .createReadStream(argv.addForeignKeyToFile)
    .pipe(csv({ separator: argv.delimiter }))
    .on("data", (row) => {
      const key = row[argv.addForeignKeyColumn];
      if (foreignKeyvalues[key]) {
        console.warn("Found duplicate: " + key);
      }

      foreignKeyvalues[key] = ++pkCounter;
    })
    .on("end", () => {
      andThen();
    });
}

function checkForeignKeyArgs() {
  if (!argv.addForeignKeyColumn || !argv.addForeignKeyToFile) {
    console.error(
      "\x1b[31mFAILED\x1b[0m Requires both arguments to be set: addForeignKeyToFile and addForeignKeyColumn"
    );
    process.exit(1);
  }
}
