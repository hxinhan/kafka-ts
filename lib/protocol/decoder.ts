import { SmartBuffer } from 'smart-buffer'

export class Decoder {
    private readonly smartBuffer: SmartBuffer

    constructor(data: any) {
        this.smartBuffer = SmartBuffer.fromBuffer(data)
    }

    readInt16() {
        return this.smartBuffer.readInt16BE()
    }

    readInt32() {
        return this.smartBuffer.readInt32BE()
    }

    readString() {
        const byteLength = this.readInt16()

        if (byteLength === -1) {
            return null
        }
        return this.smartBuffer.readString(byteLength)
    }
}