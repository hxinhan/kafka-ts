import { APIVersionBase } from './version-0'

export const apiVersionsAPI = {
    0: () => {
        const apiVersionV0 = new APIVersionBase()
        return {
            encode: (clientId: string, correlationId: number) => apiVersionV0.encode(clientId, correlationId),
            decode: (response: Buffer) => apiVersionV0.decode(response)
        }
    }
}
