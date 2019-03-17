import { SmartBuffer } from 'smart-buffer'

export class Encoder {
    private readonly smartBuffer: SmartBuffer

    constructor() {
        this.smartBuffer = new SmartBuffer()
    }

    writeInt16(value: number) {
        this.smartBuffer.writeInt16BE(value)
        return this
    }

    writeInt32(value: number) {
        this.smartBuffer.writeInt32BE(value)
        return this
    }

    writeString(value: string) {
        this.smartBuffer.writeString(value)
        return this
    }

    toBuffer() {
        return this.smartBuffer.toBuffer()
    }
}