import { API_VERSION, REQUEST_TYPE } from '../request'
import { Encoder } from '../encoder'
import { Decoder } from '../decoder';


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

export function encode(clientId: string, correlationId: number) {
    return encodeRequestWithLength(encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.apiVersions).toBuffer())
}

export function decode(response: any) {
    const decoder = new Decoder(response)
    const messageSize = decoder.readInt32()
    const correlationId = decoder.readInt32()
    const errorCode = decoder.readInt16()

    const apiVersionDecoder = (d: Decoder) => (
        {
            apiKey: d.readInt16(),
            minVersion: d.readInt16(),
            maxVersion: d.readInt16()
        }
    )

    const apiNum = decoder.readInt32()
    const apiVersions = []
    for (let i = 0; i < apiNum; i++) {
        apiVersions.push(apiVersionDecoder(decoder))
    }

    return {
        messageSize,
        correlationId,
        errorCode,
        apis: apiVersions
    }
}

export const api = { encode, decode }
