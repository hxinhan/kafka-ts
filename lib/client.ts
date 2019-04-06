import { KafkaBroker } from './cluster/broker'

const host = '127.0.0.1'
const port = 9092

const broker = new KafkaBroker(host, port)

async function test() {
    await broker.connect()
    await broker.apiVersions()
}

/*
import { SmartBuffer } from 'smart-buffer'
import { metadataAPI } from './protocol/metadata'
import { apiVersionsAPI } from './protocol/apiVersions'
import { INT32_SIZE } from './protocol/decoder'
import { Connection } from './network/connection'
import { logger } from './logger'

const connection = new Connection(host, port)
const outstandingRequests = new Map()

const metadataAPIHandler = metadataAPI.version[4]()
const apiVersionsAPIHandler = apiVersionsAPI.version[0]()

async function test() {
    await connection.connect()

    connection.data$().subscribe((data) => onData(data))

    let correlationId = 1
    let requestPayload = apiVersionsAPIHandler.encode(clientId, correlationId)
    outstandingRequests.set(correlationId, apiVersionsAPIHandler)
    connection.send(requestPayload)

    correlationId++
    requestPayload = metadataAPIHandler.encode(clientId, correlationId, [])
    outstandingRequests.set(correlationId, metadataAPIHandler)
    connection.send(requestPayload)
}

function onData(data: Buffer) {
    const sb = SmartBuffer.fromBuffer(data)
    const messageSize = sb.readInt32BE()
    const correlationId = sb.readInt32BE()

    const handler = outstandingRequests.get(correlationId)
    if (handler) {
        const remainingBytes = sb.toBuffer().slice(INT32_SIZE * 2, messageSize + INT32_SIZE * 2)
        const decodedData = handler.decode(remainingBytes)
        logger.debug('DecodedData: ', JSON.stringify(decodedData, null, 2))
    }
}
*/
test()
