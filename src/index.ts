import * as csv from 'fast-csv'
import { createWriteStream, readFile, writeFileSync } from 'fs-extra'
import path from 'path'

interface IBinarySchema {
  header: string
  padding: string
  fields: { name: string; bytes: number; type?: 'int32le' }[]
}

interface IRemoteRecord {
  id: string
  email: string
  dateOfBirth: string
  isActive: number | null
  phoneNumber: string
  firstName: string
  lastName: string
  postalCode: string
}

interface ILocalRecord {
  id: string
  email: string
  dateOfBirth: Date | null
  isActive: boolean | null
  phoneNumber: string
  firstName: string
  lastName: string
  postalCode: string
}

// local shim for logtail, etc
const logger = {
  log: async (message: string) => console.log(message),
  error: async (message: string) => console.error(message)
}

const readBinaryFile = async (filePath: string): Promise<Buffer> => {
  try {
    return readFile(filePath)
  } catch (e: any) {
    await logger.error(`Error reading binary file (${filePath}): ${e.message}`)
    throw e // rethrow to Sentry, etc
  }
}

async function parseBinaryData<T>({ schema, buffer }: { schema: IBinarySchema; buffer: Buffer }): Promise<T[]> {
  const initialOffset = Buffer.byteLength(schema.header, 'utf8')
  const paddingRe = new RegExp(schema.padding, 'g')
  const recordSize = schema.fields.reduce((acc, field) => acc + field.bytes, 0)
  const records: T[] = []

  for (let offset = initialOffset; offset < buffer.length; offset += recordSize) {
    let record: Record<string, unknown> = {}
    let cursor = offset

    for (const field of schema.fields) {
      if (field.type === 'int32le') {
        record[field.name] = buffer.readInt32LE(cursor)
      } else {
        record[field.name] = buffer.toString('utf8', cursor, cursor + field.bytes).replace(paddingRe, '')
      }

      cursor += field.bytes
    }

    records.push(record as T)
  }

  return records
}

const formatBinaryData = ({
  schema,
  records
}: {
  schema: IBinarySchema
  records: Record<string, string>[]
}): Buffer => {
  const recordSize = schema.fields.reduce((acc, field) => acc + field.bytes, 0)
  const buffer = Buffer.alloc(recordSize * records.length)
  let cursor = schema.header.length

  for (let i = 0; i < records.length; i++) {
    buffer.write(schema.header, 0, cursor)

    for (const field of schema.fields) {
      if (field.type === 'int32le' && records[i][field.name] !== null) {
        buffer.writeInt32LE(parseInt(records[i][field.name]), cursor)
      } else {
        const value = (records[i][field.name] ?? '').padEnd(field.bytes, schema.padding)
        buffer.write(value, cursor, field.bytes)
      }

      cursor += field.bytes
    }
  }

  return buffer
}

const validateBinaryData = ({
  schema,
  sourceData,
  records
}: {
  schema: IBinarySchema
  sourceData: Buffer
  records: Record<string, any>[]
}): { isValid: boolean; targetData: Buffer } => {
  // if we can regenerate the source binary file from the transformed data then there's been no data loss,
  // and content we're opinionated about was properly parsed (e.g., dates, numbers)
  const targetData = formatBinaryData({ schema, records })
  return { isValid: sourceData.compare(targetData) === 0, targetData }
}

const transformRemoteRecord = (record: IRemoteRecord): ILocalRecord => ({
  id: record.id,
  email: record.email,
  dateOfBirth: record.dateOfBirth.trim().length === 10 ? new Date(record.dateOfBirth) : null,
  isActive: record.isActive === 1 ? true : record.isActive === -1 ? false : null,
  phoneNumber: record.phoneNumber,
  firstName: record.firstName,
  lastName: record.lastName,
  postalCode: record.postalCode
})

const createRemoteRecord = (record: ILocalRecord): IRemoteRecord => ({
  id: record.id,
  email: record.email,
  dateOfBirth: record.dateOfBirth?.toISOString().slice(0, 10) ?? '',
  isActive: record.isActive === null ? 0 : record.isActive ? 1 : -1,
  phoneNumber: record.phoneNumber,
  firstName: record.firstName,
  lastName: record.lastName,
  postalCode: record.postalCode
})

const writeCsv = (records: ILocalRecord[], filePath: string) => {
  const csvStream = csv.format({
    headers: ['ID', 'Email', 'Date of Birth', 'Is Active', 'Phone Number', 'First Name', 'Last Name', 'Postal Code'],
    transform: (record: ILocalRecord) => [
      record.id,
      record.email,
      record.dateOfBirth?.toISOString().slice(0, 10),
      record.isActive,
      record.phoneNumber,
      record.firstName,
      record.lastName,
      record.postalCode
    ]
  })

  const writableStream = createWriteStream(filePath)

  return new Promise(resolve => {
    csvStream.pipe(writableStream).on('close', resolve)

    for (const record of records) {
      csvStream.write(record)
    }

    csvStream.end()
  })
}

const syncData = async () => {
  logger.log('Syncing data...')

  const schema: IBinarySchema = {
    header: '',
    padding: '\x00',
    fields: [
      { name: 'id', bytes: 4, type: 'int32le' },
      { name: 'firstName', bytes: 32 },
      { name: 'lastName', bytes: 32 },
      { name: 'dateOfBirth', bytes: 10 },
      { name: 'email', bytes: 66 },
      { name: 'isActive', bytes: 4, type: 'int32le' },
      { name: 'phoneNumber', bytes: 12 },
      { name: 'postalCode', bytes: 5 }
    ]
  }

  const inputFilePath = path.join(process.cwd(), 'data.bin')
  const debugFilePath = path.join(process.cwd(), 'debug.bin')
  const outputFilePath = path.join(process.cwd(), 'output.csv')

  const buffer = await readBinaryFile(inputFilePath)
  const rawData = await parseBinaryData<IRemoteRecord>({ buffer, schema })
  const data = rawData.map(transformRemoteRecord)

  const { isValid, targetData } = validateBinaryData({
    sourceData: buffer,
    records: data.map(createRemoteRecord),
    schema
  })

  if (!isValid) {
    writeFileSync(debugFilePath, targetData)
    logger.error(`Missing or invalid data. Check diff between ${inputFilePath} and ${debugFilePath})`)
    return
  }

  await writeCsv(data, outputFilePath)

  logger.log('Synced data')
}

syncData()
