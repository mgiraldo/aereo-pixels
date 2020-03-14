var express = require('express')
var router = express.Router()

const fs = require('fs')
const { exec } = require('child_process')

const { Client } = require('pg')
const client = new Client({
  // user: 'dbuser',
  // host: 'database.server.com',
  database: 'nsw_data'
})
client.connect()

const TILE_SIZE = 32
const MAX_QUERY_LIMIT = 4096
const BASE_PATH = '/Users/mga/Documents/projects/nsw'
const MINI_FOLDER = '32_32'
const BIG_FOLDER = 'files'

const pathForFile = (type, filename, size) => {
  const path = `${type === 'relative' ? '.' : BASE_PATH}/${
    size === 'mini' ? MINI_FOLDER : BIG_FOLDER
  }/${filename.substr(0, 4)}/${filename.substring(0, filename.indexOf('.'))}.${
    size === 'mini' ? 'png' : 'jpg'
  }`
  return path
}

const relativePathForFile = (filename, size = 'mini') => {
  return pathForFile('relative', filename, size)
}

const absolutePathForFile = (filename, size = 'mini') => {
  return pathForFile('absolute', filename, size)
}

const asyncForEach = async (array, callback) => {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array)
  }
}

const makeIdQuery = async (ids) => {
  const dollars = ids.map((_, index) => '($' + (index + 1) + ')').join(',')
  const data = await client.query(
    'SELECT id, filename FROM file_mga INNER JOIN ( VALUES ' +
      dollars +
      ' ) vals(v) ON (id = v)',
    ids
  )
  return data
}

const createPixelsForBucket = async (bucket) => {}

const importColorsForBucket = async (bucket) => {
  const queryLimit = MAX_QUERY_LIMIT // ids at a time
  const ids = [...bucket.ids]

  const groups = []
  while (ids.length) {
    groups.push(ids.splice(0, queryLimit))
  }

  groups.forEach((group) => {
    // for each file (id.json)
    const dollars = []
    const values = []
    let count = 0
    group.forEach((id) => {
      const f = `${BASE_PATH}/data/colors_output_jq/${id}.json`
      let str = ''
      try {
        str = fs.readFileSync(f, 'utf8')
      } catch {
        console.log('skipped', id)
        return
      }
      if (str.length === 0) {
        console.log('skipped', id)
        return
      }
      const data = JSON.parse(str)
      // get the colors
      const palette_colors = data.map((d) => `${d.h}:${d.f}`).join(',')
      const palette_text = data.map((d) => d.t).join(',')
      values.push(palette_colors, palette_text, id)
      count++
    })
    for (let i = 0; i < count; i++) {
      dollars.push(`($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`)
    }
    // put them in db
    const sql = `UPDATE file_mga as f SET 
    palette_colors = c.palette_colors,
    palette_text = c.palette_text
    FROM (
      VALUES
      ${dollars.join(',')}
    ) AS c(palette_colors, palette_text, id)
    WHERE c.id = f.id`
    client.query(sql, values)
  })

  return true
}

const createAtlasForBucket = async (bucket) => {
  const queryLimit = MAX_QUERY_LIMIT // ids at a time
  const ids = [...bucket.ids]
  const key = bucket.key

  const groups = []
  while (ids.length) {
    groups.push(ids.splice(0, queryLimit))
  }

  const w = TILE_SIZE
  let groupIdx = 0
  const atlas = []

  await asyncForEach(groups, async (group) => {
    const data = await makeIdQuery(group)

    const rows = data.rows
    const paths = []
    const count = rows.length
    const side = Math.ceil(Math.sqrt(count))

    group.forEach((id, index) => {
      const result = rows.find((row) => row.id === id)
      let input = ''
      if (result) {
        const path = relativePathForFile(result.filename)
        if (path.indexOf(' ') === -1 && path.indexOf('(') === -1) {
          input = path
        } else {
          input = './server/public/images/blank.png'
        }
      } else {
        input = './server/public/images/blank.png'
      }
      paths.push({
        input: `"${input}"`,
        top: Math.floor(index / side) * w,
        left: (index % side) * w
      })
    })
    const atlasName = `${key}_${groupIdx}.jpg`
    const cmd = `cd ${BASE_PATH}; montage ${paths
      .map((p) => p.input)
      .join(
        ' '
      )}  -geometry +0+0 -background none -tile ${side}x${side} ./server/public/atlas/${atlasName}`
    // console.log(cmd)
    await exec(cmd, (error, stdout, stderr) => {
      if (error) {
        console.error(`exec error ${error}`)
        return
      }
    })
    atlas.push(atlasName)
    groupIdx++
  })
  return atlas
}

router.all('/atlas', async (req, res, next) => {
  const bucket = req.body.bucket

  const data = await client.query('SELECT bucket, file_ids FROM bucket_ids')

  let buckets = []

  data.rows.sort(
    (a, b) => b.file_ids.split(',').length - a.file_ids.split(',').length
  )

  data.rows.forEach((row) => {
    key = row.bucket
    ids = row.file_ids.split(',')
    buckets.push({ key, atlas: [], ids })
  })

  if (bucket) {
    const idx = buckets.findIndex((b) => b.key === bucket)
    buckets[idx].atlas = await createAtlasForBucket(buckets[idx])
  }

  res.render('atlas', { data: { bucket, buckets } })
})

router.all('/pixels', async (req, res, next) => {
  const bucket = req.body.bucket
  const db = req.query.db

  const data = await client.query('SELECT bucket, file_ids FROM bucket_ids')

  let buckets = []

  data.rows.sort(
    (a, b) => b.file_ids.split(',').length - a.file_ids.split(',').length
  )

  data.rows.forEach((row) => {
    key = row.bucket
    ids = row.file_ids.split(',')
    buckets.push({ key, pixels: [], ids, db: false })
  })

  if (bucket) {
    const idx = buckets.findIndex((b) => b.key === bucket)
    buckets[idx].pixels = await createPixelsForBucket(buckets[idx])
  }

  if (db) {
    const idx = buckets.findIndex((b) => b.key === db)
    if (idx !== -1) buckets[idx].db = await importColorsForBucket(buckets[idx])
  }

  res.render('pixels', { data: { bucket, buckets } })
})

router.get('/:number/:filename', async (req, res, next) => {
  const options = {
    dotfiles: 'deny',
    headers: {
      'x-timestamp': Date.now(),
      'x-sent': true
    }
  }

  const path = `${BASE_PATH}/${BIG_FOLDER}/${req.params.number}/${req.params.filename}`
  res.sendFile(path, options, function(err) {
    if (err) {
      next(err)
    } else {
      console.log('sent:', path)
    }
  })
})

router.get('/:id', async (req, res, next) => {
  const options = {
    dotfiles: 'deny',
    headers: {
      'x-timestamp': Date.now(),
      'x-sent': true
    }
  }

  const data = await client.query(
    'SELECT filename FROM file_mga WHERE id = $1',
    [req.params.id]
  )

  const row = data.rows[0]
  const filename = row.filename

  const path = absolutePathForFile(filename, req.query.s)
  res.sendFile(path, options, function(err) {
    if (err) {
      next(err)
    } else {
      console.log('sent:', path)
    }
  })
})

module.exports = router
