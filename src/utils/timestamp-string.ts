import moment from "moment";

export function timestampString() {
    return moment().format("YYYYMMDD_HHmmss")
}