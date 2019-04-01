
import { SmartBuffer } from 'smart-buffer'
import { metadataAPI } from './protocol/metadata'
import { apiVersionsAPI } from './protocol/apiVersions'
import { INT32_SIZE } from './protocol/decoder'
import { Connection } from './network/connection'
import { logger } from './logger'

const clientName = 'test'
const host = '127.0.0.1'
const port = 9092
const connection = new Connection(host, port)
const requestQueue = new Map()

const metadataAPIHandler = metadataAPI['0']()
const apiVersionsAPIHandler = apiVersionsAPI['0']()

async function test() {
    await connection.connect()

    connection.data$().subscribe((data) => onData(data))

    let correlationId = 1
    let request = apiVersionsAPIHandler.encode(clientName, correlationId)
    requestQueue.set(correlationId, apiVersionsAPIHandler)
    connection.send(request)

    correlationId++
    request = metadataAPIHandler.encode(clientName, correlationId, [])
    requestQueue.set(correlationId, metadataAPIHandler)
    connection.send(request)
}

function onData(data: Buffer) {
    const sb = SmartBuffer.fromBuffer(data)
    const messageSize = sb.readInt32BE()
    const correlationId = sb.readInt32BE()

    const handler = requestQueue.get(correlationId)
    if (handler) {
        const remainingBytes = sb.toBuffer().slice(INT32_SIZE * 2, messageSize + INT32_SIZE * 2)
        const decodedData = handler.decode(remainingBytes)
        logger.debug('DecodedData: ', JSON.stringify(decodedData, null, 2))
    }
}

test()
