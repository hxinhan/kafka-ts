import { API } from '../api'
import { REQUEST_TYPE } from '../request'

interface APIVersion {
    apiKey: number
    minVersion: number
    maxVersion: number
}

interface APIVersionsResponse {
    errorCode: number
    apiVersions: APIVersion[]
}

export class APIVersionBase extends API<{}, APIVersionsResponse> {
    protected apiVersion: number

    constructor() {
        super()
        this.apiVersion = 0
    }


    /**
     * Encode apiVersions request
     *
     * @param {string} clientId
     * @param {number} correlationId
     * @returns
     * @memberof APIVersionBase
     */
    encode(clientId: string, correlationId: number) {
        return this.encodeRequest(clientId, correlationId, REQUEST_TYPE.apiVersions, 0)
    }

    /**
     * Decode apiVersions response
     *
     * @param {Buffer} response
     * @returns {APIVersionsResponse}
     * @memberof APIVersionBase
     */
    decode(response: Buffer): APIVersionsResponse {
        this.decoder.fromBuffer(response)
        const errorCode = this.decoder.readInt16()

        const apiVersions = this.decoder.readArray<APIVersion>(() => (
            {
                apiKey: this.decoder.readInt16(),
                minVersion: this.decoder.readInt16(),
                maxVersion: this.decoder.readInt16()
            }
        ))

        return { errorCode, apiVersions }
    }
}