import { API_VERSION } from './request'
import { Encoder } from './encoder'
import { Decoder } from './decoder'

export interface APIResponseBase {
    messageSize: number
    correlationId: number
}

export abstract class API {

    protected abstract apiVersion: number
    protected readonly encoder: Encoder
    protected readonly decoder: Decoder

    constructor() {
        this.encoder = new Encoder()
        this.decoder = new Decoder()
    }

    protected encodeRequestWithLength(request: any) {
        return Buffer.concat([new Encoder()
            .writeInt32(request.length)
            .toBuffer(), request])
    }

    protected encodeRequestHeader(clientId: string, correlationId: number, apiKey: number, apiVersion = API_VERSION) {
        return this.encoder
            .writeInt16(apiKey)
            .writeInt16(apiVersion)
            .writeInt32(correlationId)
            .writeInt16(clientId.length)
            .writeString(clientId)
    }

    protected encodeRequest(clientId: string, correlationId: number, requestType: number, apiVersion?: number) {
        return this.encodeRequestWithLength(
            this.encodeRequestHeader(clientId, correlationId, requestType, apiVersion).toBuffer()
        )
    }

    protected decodeResponse(response: Buffer): APIResponseBase {
        this.decoder.fromBuffer(response)
        const messageSize = this.decoder.readInt32()
        const correlationId = this.decoder.readInt32()

        return { messageSize, correlationId }
    }

    /**
     * Encode API request to buffer
     *
     * @abstract
     * @param {string} clientId
     * @param {number} correlationId
     * @param {object} [params]
     * @returns {Buffer}
     * @memberof API
     */
    abstract encode(clientId: string, correlationId: number, params?: object): Buffer

    /**
     * Decode buffer to JSON format
     *
     * @abstract
     * @param {Buffer} response
     * @returns {object}
     * @memberof API
     */
    abstract decode(response: Buffer): object

}
