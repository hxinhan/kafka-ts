import { BrokerMetadataBase, TopicMetadataBase, MetadataV0, PartitionMetadata, MetadataResponseV0 } from './version-0'

export interface MetadataResponseV1 extends MetadataResponseV0 {
    controllerId: number
    brokers: BrokerMetadataV1[]
    topics: TopicMetadataV1[]
}

export interface BrokerMetadataV1 extends BrokerMetadataBase {
    rack: string | null
}

export interface TopicMetadataV1 extends TopicMetadataBase {
    isInternal: boolean
}

export class MetadataV1 extends MetadataV0 {
    constructor() {
        super()
        this.apiVersion = 1
    }

    /**
     * Decode metadata response
     *
     * @param {Buffer} response
     * @returns {MetadataResponseV4}
     * @memberof MetadataV1
     */
    decode(response: Buffer): MetadataResponseV1 {
        this.decoder.fromBuffer(response)
        const brokersReader = () => (
            {
                nodeId: this.decoder.readInt32(),
                host: this.decoder.readString(),
                port: this.decoder.readInt32(),
                rack: this.decoder.readString()
            }
        )
        const topicsReader = () => (
            {
                errorCode: this.decoder.readInt16(),
                topic: this.decoder.readString() as string,
                isInternal: this.decoder.readBoolean(),
                partitions: this.decoder.readArray<PartitionMetadata>(partitionsReader).filter((p) => !p.errorCode)
            }
        )
        const partitionsReader = () => ({
            errorCode: this.decoder.readInt16(),
            partition: this.decoder.readInt32(),
            leader: this.decoder.readInt32(),
            replicas: this.decoder.readArray(() => this.decoder.readInt32()),
            isr: this.decoder.readArray(() => this.decoder.readInt32()),
        })

        return {
            brokers: this.decoder.readArray<BrokerMetadataV1>(brokersReader),
            controllerId: this.decoder.readInt32(),
            topics: this.decoder.readArray<TopicMetadataV1>(topicsReader).filter((p) => !p.errorCode)
        }
    }
}
