const fs = require('fs');

import {
  Connection,
  createConnection,
  createConnectionPool,
  ConnectionPool
} from '../src/engine';

import { Schema } from '../src/model';
import { Database } from '../src/database';

const DB_TYPE = process.env.DB_TYPE || 'mysql';
const DB_HOST = process.env.DB_HOST || 'localhost';
const DB_USER = process.env.DB_USER || 'root';
const DB_PASS = process.env.DB_PASS || 'secret';
const DB_NAME = process.env.DB_NAME || 'sqlit_test';

const SCHEMA = fs.readFileSync('test/data/schema.sql').toString();
const DATA = fs.readFileSync('test/data/data.sql').toString();

function createMySQLDatabase(name: string, data = true): Promise<any> {
  const mysql = require('mysql');
  const database = `${DB_NAME}_${name}`;

  const db = mysql.createConnection({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASS
  });

  const sql = SCHEMA + (data ? DATA : '');

  const lines = [
    `drop database if exists ${database}`,
    `create database ${database}`,
    `use ${database}`
  ].concat(sql.split(';').filter(line => line.trim()));

  return serialise(line => {
    return new Promise((resolve, reject) => {
      db.query(line, (error, results, fields) => {
        if (error) reject(Error(error));
        resolve();
      });
    });
  }, lines);
}

function dropMySQLDatabase(name: string): Promise<void> {
  const mysql = require('mysql');
  const database = `${DB_NAME}_${name}`;

  const db = mysql.createConnection({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASS
  });

  return new Promise(resolve => {
    db.query(`drop database if exists ${database}`, err => {
      if (err) throw err;
      resolve();
    });
  });
}

function createMySQLConnection(name: string): Connection {
  const database = `${DB_NAME}_${name}`;
  return createConnection('mysql', {
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASS,
    database: database,
    timezone: 'Z',
    connectionLimit: 10
  });
}

function serialise(func, argv: any[]) {
  return new Promise(resolve => {
    const results = [];
    let next = 0;
    function _resolve() {
      if (next >= argv.length) {
        resolve(results);
      } else {
        const args = argv[next++];
        func(args).then(result => {
          results.push(result);
          _resolve();
        });
      }
    }
    _resolve();
  });
}

export function getExampleData() {
  const fileName = require('path').join(
    __dirname,
    '..',
    'test',
    'data',
    'schema.json'
  );
  return JSON.parse(fs.readFileSync(fileName).toString());
}

export function createDatabase(name: string, data = true): Promise<any> {
  return createMySQLDatabase(name, data);
}

export function dropDatabase(name: string): Promise<any> {
  return dropMySQLDatabase(name);
}

export function createTestConnection(name: string): Connection {
  return createMySQLConnection(name);
}

export function createTestConnectionPool(name: string): ConnectionPool {
  const database = `${DB_NAME}_${name}`;
  return createConnectionPool('mysql', {
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASS,
    database: database,
    timezone: 'Z',
    connectionLimit: 10
  });
}

export function connectToDatabase(name: string, schema?: Schema): Database {
  if (!schema) {
    schema = new Schema(getExampleData());
  }
  const pool = createTestConnectionPool(name);
  return new Database(pool, schema);
}

export function getId(length: number = 8) {
  return Math.random()
    .toString(36)
    .substring(length);
}

export function getDatabaseName(name: string): string {
  return `${DB_NAME}_${name}`;
}
