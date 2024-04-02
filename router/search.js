const express = require('express')
const router = express.Router()
const SearchController = require('../controller/SearchController.js')

// 搜索接口
router.get('/list', SearchController.list)

module.exports = router