// tslint:disable:no-console

export const logger = {
    info(msg: string, obj?: any) {
        if (obj) {
            console.info(msg, obj)
        } else {
            console.info(msg)
        }
    },
    warn(msg: string, obj?: any) {
        if (obj) {
            console.warn(msg, obj)
        } else {
            console.warn(msg)
        }
    },
    error(msg: string, obj?: any) {
        if (obj) {
            console.error(msg, obj)
        } else {
            console.error(msg)
        }
    },
    debug(msg: string, obj?: any) {
        if (obj) {
            console.log(msg, obj)
        } else {
            console.log(msg)
        }
    }
}