
class KafkaTsError extends Error {
    constructor(message: string) {
        super(message)
        this.message = message
        this.name = this.constructor.name
    }
}

export class ConnectionError extends KafkaTsError {
    readonly broker: string
    readonly errorCode?: string

    constructor(message: string, obj: { broker: string, errorCode?: string }) {
        super(message)
        this.broker = obj.broker
        this.errorCode = obj.errorCode
    }
}
