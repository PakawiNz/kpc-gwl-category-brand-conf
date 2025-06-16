// production
// const HOST = 'open-api-powerpass.kingpower.com'
// const CLIENT_ID = 'core-data-importer'
// const CLIENT_SECRET = 'Mft85ovgKPAzrQTIbNB9QtHToidT1CSl'
// const ENDPOINT_GET_TOKEN = 'https://iam.kingpower.com/realms/gwl/protocol/openid-connect/token'

const HOST = 'dev-api-gateway.gwl.kpc-dev.com'
const CLIENT_ID = 'core-data-importer'
const CLIENT_SECRET = '9vEuQa3PBsRnAtBuWx7JmwVCwuDACD4G'
const ENDPOINT_GET_TOKEN = `https://iam.kingpower.com/realms/dev-gwl/protocol/openid-connect/token`

async function getToken() {
    console.log('🔫🔫🔫 get token')
    const response = await fetch(ENDPOINT_GET_TOKEN, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: new URLSearchParams({
            grant_type: 'client_credentials',
            client_id: CLIENT_ID,
            client_secret: CLIENT_SECRET
        })
    })
    const data = await response.json()
    if (response.status != 200) throw data
    console.log(data)
    return data
}

// get channels id
async function getChannel(accessToken, codeKeyword) {
    try {
        console.log('🔫🔫🔫 get channel')
        const response = await fetch(`https://${HOST}/admin/channels?searchBy=CODE&searchKeyword=${codeKeyword}&perPage=100`, {
            method: 'GET',
            headers: {
                Authorization: `Bearer ${accessToken}`
            },
        })
        const data = await response.json()
        console.log(data)
        return data
    } catch (error) {
        console.error(error)
    }
}

async function getProduct(accessToken, channelId) {
    try {
        console.log('🔫🔫🔫 get product')
        const response = await fetch(`https://${HOST}/admin/channels/${channelId}/products`, {
            method: 'GET',
            headers: {
                Authorization: `Bearer ${accessToken}`
            },
        })
        const data = await response.json()
        console.log(data)
        return data
    } catch (error) {
        console.error(error)
    }
}

async function main() {
    const result = await getToken()
    const channels = await getChannel(result['access_token'], 'KPC')
    console.log(Object.fromEntries(channels.data.map(channel => [channel.code, channel.id])))
}

main()


// https://iam.kingpower.com/realms/gwl/.well-known/openid-configuration

// function 