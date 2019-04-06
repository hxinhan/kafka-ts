import { APIVersionBase } from './version-0'

const api = {
    0: () => {
        const apiVersionV0 = new APIVersionBase()
        return {
            encode: (clientId: string, correlationId: number) => apiVersionV0.encode(clientId, correlationId),
            decode: (response: Buffer) => apiVersionV0.decode(response)
        }
    }
}

export const apiVersionsAPI = {
    versions: Object.keys(api).map((v) => parseInt(v)),
    version: api
}
