/**
 * @param list 搜索接口
 */

const SearchService = require('../service/SearchService.js')

const SearchController = {
    list: async (req, res) => {
        let handleRes = await SearchService.list(req)
        res.send(handleRes)
    }
}

module.exports = SearchController