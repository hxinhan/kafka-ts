import * as net from 'net'
import { Encoder } from './api/encoder'
import { Decoder } from './api/decoder'
import { REQUEST_TYPE, API_VERSION } from './protocol/request'

const clientName = 'test'
const host = '127.0.0.1'
const port = 9092

const socket = net.createConnection(port, host)
console.log('connecting to 127.0.0.1:9092')

socket.on('connect', () => {
    console.log('connected')
    // socket.write(sendApiVersionRequest())
    socket.write(sendMetadataRequest())
})
socket.on('data', (data) => {
    // console.log('data: ', data)
    // const decodedData = decodeVersionsResponse(data)
    const decodedData = decodeMetadataResponse(data)
    console.log('decodedData: ', decodedData)
})
socket.on('error', (err) => {
    console.log('error: ', err)
})
socket.on('close', () => {
    console.log('closed')
})

function encodeRequestWithLength(request: any) {
    return Buffer.concat([new Encoder()
        .writeInt32(request.length)
        .toBuffer(), request])
}

function encodeRequestHeader(clientId: string, correlationId: number, apiKey: number, apiVersion = API_VERSION) {
    return new Encoder()
        .writeInt16(apiKey)
        .writeInt16(apiVersion)
        .writeInt32(correlationId)
        .writeInt16(clientId.length)
        .writeString(clientId)
}

function sendApiVersionRequest() {
    return encodeVersionsRequest(clientName, 100)
}

function encodeVersionsRequest(clientId: string, correlationId: number) {
    return encodeRequestWithLength(encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.apiVersions).toBuffer())
}


function sendMetadataRequest() {
    return encodeMetadataRequest(clientName, 100)
}

function encodeMetadataRequest(clientId: string, correlationId: number) {
    const request = encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.metadata)
    const topics: string[] = []

    request.writeInt32(topics.length)

    topics.forEach((topic) => {
        request.writeInt16(topic.length)
            .writeString(topic)
    })

    return encodeRequestWithLength(request.toBuffer())
}

function decodeMetadataResponse(response: any) {
    const decoder = new Decoder(response)
    const messageSize = decoder.readInt32()
    const correlationId = decoder.readInt32()
    console.log('!!!! messageSize: ', messageSize)
    console.log('!!!! correlationId: ', correlationId)

    const brokerNum = decoder.readInt32()
    console.log('!!!! brokerNum: ', brokerNum)

    const brokers = []
    const brokerDecoder = (d: Decoder) => (
        {
            nodeId: d.readInt32(),
            nodeHost: d.readString(),
            nodePort: d.readInt32()
        }
    )
    for (let i = 0; i < brokerNum; i++) {
        brokers.push(brokerDecoder(decoder))
    }
    console.log(brokers)
}
