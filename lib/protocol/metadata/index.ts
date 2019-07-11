import { MetadataV0 } from './version-0'
import { MetadataV1 } from './version-1'
import { MetadataV2 } from './version-2'
import { MetadataV3 } from './version-3'
import { MetadataV4 } from './version-4'

const api = {
    0: () => {
        const metadataV0 = new MetadataV0()
        return {
            encode: (clientId: string, correlationId: number, topics: string[]) =>
                metadataV0.encode(clientId, correlationId, { topics }
                ),
            decode: (response: Buffer) => metadataV0.decode(response)
        }
    },
    1: () => {
        const metadataV1 = new MetadataV1()
        return {
            encode: (clientId: string, correlationId: number, topics: string[]) =>
                metadataV1.encode(clientId, correlationId, { topics }
                ),
            decode: (response: Buffer) => metadataV1.decode(response)
        }
    },
    2: () => {
        const metadataV2 = new MetadataV2()
        return {
            encode: (clientId: string, correlationId: number, topics: string[]) =>
                metadataV2.encode(clientId, correlationId, { topics }
                ),
            decode: (response: Buffer) => metadataV2.decode(response)
        }
    },
    3: () => {
        const metadataV3 = new MetadataV3()
        return {
            encode: (clientId: string, correlationId: number, topics: string[]) =>
                metadataV3.encode(clientId, correlationId, { topics }
                ),
            decode: (response: Buffer) => metadataV3.decode(response)
        }
    },
    4: () => {
        const metadataV4 = new MetadataV4()
        return {
            encode: (clientId: string, correlationId: number, topics: string[], allowAutoTopicCreation = false) =>
                metadataV4.encode(clientId, correlationId, { topics, allowAutoTopicCreation }
                ),
            decode: (response: Buffer) => metadataV4.decode(response)
        }
    }
}

export const metadataAPI = {
    versions: Object.keys(api).map((v) => parseInt(v)),
    version: 4,
    api
}
