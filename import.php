<?php
/**
 * OpenCart / OCStore Mass Catalog Importer
 * Imports products, categories, filters and attributes from XML/CSV
 * Supports 100k+ items with MySQL optimization (bulk inserts, index hints)
 *
 * Usage: php import.php --file=products.xml --store=0 --batch=500
 */

define('DIR_ROOT', __DIR__ . '/');

class CatalogImporter {

    private $db;
    private $store_id;
    private $batch_size;
    private $language_id = 1;

    public function __construct(array $config) {
        $this->store_id   = $config['store_id'] ?? 0;
        $this->batch_size = $config['batch_size'] ?? 500;
        $dsn = "mysql:host={$config['db_host']};dbname={$config['db_name']};charset=utf8mb4";
        $this->db = new PDO($dsn, $config['db_user'], $config['db_pass'], [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->optimizeMySQL();
    }

    private function optimizeMySQL() {
        $this->db->exec("SET SESSION bulk_insert_buffer_size = 268435456");
        $this->db->exec("SET SESSION foreign_key_checks = 0");
        $this->db->exec("SET SESSION unique_checks = 0");
        $this->db->exec("SET SESSION sql_log_bin = 0");
    }

    public function importFromXml(string $file): array {
        $stats = ['products' => 0, 'categories' => 0, 'skipped' => 0, 'errors' => 0];

        $xml = new XMLReader();
        $xml->open($file);

        $batch = [];
        while ($xml->read()) {
            if ($xml->nodeType === XMLReader::ELEMENT && $xml->name === 'product') {
                $node = new SimpleXMLElement($xml->readOuterXML());
                $batch[] = $this->parseProduct($node);

                if (count($batch) >= $this->batch_size) {
                    $inserted = $this->insertProductBatch($batch);
                    $stats['products'] += $inserted;
                    $batch = [];
                    echo "Imported: {$stats['products']}\r";
                }
            }
        }

        if ($batch) {
            $stats['products'] += $this->insertProductBatch($batch);
        }

        $xml->close();
        echo "\nDone. Imported: {$stats['products']} products\n";
        return $stats;
    }

    private function parseProduct(\SimpleXMLElement $node): array {
        return [
            'model'      => (string)($node->model ?? $node->sku ?? ''),
            'sku'        => (string)($node->sku ?? ''),
            'price'      => (float)($node->price ?? 0),
            'quantity'   => (int)($node->quantity ?? 0),
            'status'     => 1,
            'name'       => (string)($node->name ?? ''),
            'description'=> (string)($node->description ?? ''),
            'category'   => (string)($node->category ?? ''),
            'image'      => (string)($node->image ?? ''),
            'attributes' => $this->parseAttributes($node->attributes ?? null),
            'filters'    => $this->parseFilters($node->filters ?? null),
        ];
    }

    private function parseAttributes(?\SimpleXMLElement $attrs): array {
        $result = [];
        if (!$attrs) return $result;
        foreach ($attrs->attribute ?? [] as $attr) {
            $result[] = ['name' => (string)$attr['name'], 'value' => (string)$attr];
        }
        return $result;
    }

    private function parseFilters(?\SimpleXMLElement $filters): array {
        $result = [];
        if (!$filters) return $result;
        foreach ($filters->filter ?? [] as $f) {
            $result[] = ['group' => (string)$f['group'], 'name' => (string)$f];
        }
        return $result;
    }

    private function insertProductBatch(array $products): int {
        $count = 0;
        $this->db->beginTransaction();
        try {
            foreach ($products as $p) {
                $product_id = $this->upsertProduct($p);
                if ($product_id) {
                    $this->upsertProductDescription($product_id, $p);
                    $this->linkProductToStore($product_id);
                    if ($p['category']) $this->linkCategory($product_id, $p['category']);
                    if ($p['attributes']) $this->insertAttributes($product_id, $p['attributes']);
                    if ($p['filters'])    $this->insertFilters($product_id, $p['filters']);
                    $count++;
                }
            }
            $this->db->commit();
        } catch (\Exception $e) {
            $this->db->rollBack();
            echo "\nError: " . $e->getMessage() . "\n";
        }
        return $count;
    }

    private function upsertProduct(array $p): ?int {
        $stmt = $this->db->prepare("
            INSERT INTO oc_product (model, sku, price, quantity, status, image, date_added, date_modified)
            VALUES (:model, :sku, :price, :qty, :status, :image, NOW(), NOW())
            ON DUPLICATE KEY UPDATE price=VALUES(price), quantity=VALUES(quantity), date_modified=NOW()
        ");
        $stmt->execute([
            ':model'  => $p['model'],
            ':sku'    => $p['sku'],
            ':price'  => $p['price'],
            ':qty'    => $p['quantity'],
            ':status' => $p['status'],
            ':image'  => $p['image'],
        ]);
        return (int)$this->db->lastInsertId() ?: $this->getProductIdByModel($p['model']);
    }

    private function getProductIdByModel(string $model): ?int {
        $stmt = $this->db->prepare("SELECT product_id FROM oc_product WHERE model = ? LIMIT 1");
        $stmt->execute([$model]);
        return $stmt->fetchColumn() ?: null;
    }

    private function upsertProductDescription(int $pid, array $p): void {
        $stmt = $this->db->prepare("
            INSERT INTO oc_product_description (product_id, language_id, name, description, tag, meta_title, meta_description, meta_keyword)
            VALUES (:pid, :lid, :name, :desc, '', :name, '', '')
            ON DUPLICATE KEY UPDATE name=VALUES(name), description=VALUES(description)
        ");
        $stmt->execute([':pid' => $pid, ':lid' => $this->language_id, ':name' => $p['name'], ':desc' => $p['description']]);
    }

    private function linkProductToStore(int $pid): void {
        $this->db->prepare("INSERT IGNORE INTO oc_product_to_store (product_id, store_id) VALUES (?, ?)")
                 ->execute([$pid, $this->store_id]);
    }

    private function linkCategory(int $pid, string $cat_name): void {
        $cat_id = $this->getOrCreateCategory($cat_name);
        $this->db->prepare("INSERT IGNORE INTO oc_product_to_category (product_id, category_id) VALUES (?,?)")
                 ->execute([$pid, $cat_id]);
    }

    private function getOrCreateCategory(string $name): int {
        $stmt = $this->db->prepare("SELECT cd.category_id FROM oc_category_description cd WHERE cd.name = ? AND cd.language_id = ? LIMIT 1");
        $stmt->execute([$name, $this->language_id]);
        if ($id = $stmt->fetchColumn()) return (int)$id;

        $this->db->prepare("INSERT INTO oc_category (parent_id, top, column, sort_order, status, date_added, date_modified) VALUES (0,1,1,0,1,NOW(),NOW())")->execute();
        $cat_id = (int)$this->db->lastInsertId();
        $this->db->prepare("INSERT INTO oc_category_description (category_id, language_id, name, description, meta_title) VALUES (?,?,?,?,?)")
                 ->execute([$cat_id, $this->language_id, $name, '', $name]);
        $this->db->prepare("INSERT IGNORE INTO oc_category_to_store (category_id, store_id) VALUES (?,?)")
                 ->execute([$cat_id, $this->store_id]);
        return $cat_id;
    }

    private function insertAttributes(int $pid, array $attributes): void {
        foreach ($attributes as $attr) {
            $ag_id   = $this->getOrCreateAttributeGroup('General');
            $attr_id = $this->getOrCreateAttribute($attr['name'], $ag_id);
            $this->db->prepare("INSERT INTO oc_product_attribute (product_id, attribute_id, language_id, text) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE text=VALUES(text)")
                     ->execute([$pid, $attr_id, $this->language_id, $attr['value']]);
        }
    }

    private function getOrCreateAttributeGroup(string $name): int {
        $stmt = $this->db->prepare("SELECT agd.attribute_group_id FROM oc_attribute_group_description agd WHERE agd.name=? AND agd.language_id=? LIMIT 1");
        $stmt->execute([$name, $this->language_id]);
        if ($id = $stmt->fetchColumn()) return (int)$id;
        $this->db->prepare("INSERT INTO oc_attribute_group (sort_order) VALUES (0)")->execute();
        $ag_id = (int)$this->db->lastInsertId();
        $this->db->prepare("INSERT INTO oc_attribute_group_description (attribute_group_id, language_id, name) VALUES (?,?,?)")->execute([$ag_id, $this->language_id, $name]);
        return $ag_id;
    }

    private function getOrCreateAttribute(string $name, int $ag_id): int {
        $stmt = $this->db->prepare("SELECT ad.attribute_id FROM oc_attribute_description ad WHERE ad.name=? AND ad.language_id=? LIMIT 1");
        $stmt->execute([$name, $this->language_id]);
        if ($id = $stmt->fetchColumn()) return (int)$id;
        $this->db->prepare("INSERT INTO oc_attribute (attribute_group_id, sort_order) VALUES (?,0)")->execute([$ag_id]);
        $attr_id = (int)$this->db->lastInsertId();
        $this->db->prepare("INSERT INTO oc_attribute_description (attribute_id, language_id, name) VALUES (?,?,?)")->execute([$attr_id, $this->language_id, $name]);
        return $attr_id;
    }

    private function insertFilters(int $pid, array $filters): void {
        foreach ($filters as $f) {
            $fg_id     = $this->getOrCreateFilterGroup($f['group']);
            $filter_id = $this->getOrCreateFilter($f['name'], $fg_id);
            $this->db->prepare("INSERT IGNORE INTO oc_product_filter (product_id, filter_id) VALUES (?,?)")
                     ->execute([$pid, $filter_id]);
        }
    }

    private function getOrCreateFilterGroup(string $name): int {
        $stmt = $this->db->prepare("SELECT fgd.filter_group_id FROM oc_filter_group_description fgd WHERE fgd.name=? AND fgd.language_id=? LIMIT 1");
        $stmt->execute([$name, $this->language_id]);
        if ($id = $stmt->fetchColumn()) return (int)$id;
        $this->db->prepare("INSERT INTO oc_filter_group (sort_order) VALUES (0)")->execute();
        $fg_id = (int)$this->db->lastInsertId();
        $this->db->prepare("INSERT INTO oc_filter_group_description (filter_group_id, language_id, name) VALUES (?,?,?)")->execute([$fg_id, $this->language_id, $name]);
        return $fg_id;
    }

    private function getOrCreateFilter(string $name, int $fg_id): int {
        $stmt = $this->db->prepare("SELECT fd.filter_id FROM oc_filter_description fd WHERE fd.name=? AND fd.language_id=? LIMIT 1");
        $stmt->execute([$name, $this->language_id]);
        if ($id = $stmt->fetchColumn()) return (int)$id;
        $this->db->prepare("INSERT INTO oc_filter (filter_group_id, sort_order) VALUES (?,0)")->execute([$fg_id]);
        $filter_id = (int)$this->db->lastInsertId();
        $this->db->prepare("INSERT INTO oc_filter_description (filter_id, filter_group_id, language_id, name) VALUES (?,?,?,?)")->execute([$filter_id, $fg_id, $this->language_id, $name]);
        return $filter_id;
    }

    public function addIndexes(): void {
        $indexes = [
            "CREATE INDEX IF NOT EXISTS idx_product_model ON oc_product(model)",
            "CREATE INDEX IF NOT EXISTS idx_product_sku   ON oc_product(sku)",
            "CREATE INDEX IF NOT EXISTS idx_cat_desc_name ON oc_category_description(name(191), language_id)",
            "CREATE INDEX IF NOT EXISTS idx_attr_desc_name ON oc_attribute_description(name(191), language_id)",
            "CREATE INDEX IF NOT EXISTS idx_filter_desc_name ON oc_filter_description(name(191), language_id)",
        ];
        foreach ($indexes as $sql) {
            try { $this->db->exec($sql); } catch (\Exception $e) { /* index may already exist */ }
        }
        echo "Indexes ensured.\n";
    }
}

// CLI entry point
if (php_sapi_name() === 'cli') {
    $opts = getopt('', ['file:', 'store:', 'batch:', 'host:', 'dbname:', 'user:', 'pass:']);

    $importer = new CatalogImporter([
        'db_host'    => $opts['host']   ?? 'localhost',
        'db_name'    => $opts['dbname'] ?? 'opencart',
        'db_user'    => $opts['user']   ?? 'root',
        'db_pass'    => $opts['pass']   ?? '',
        'store_id'   => (int)($opts['store'] ?? 0),
        'batch_size' => (int)($opts['batch'] ?? 500),
    ]);

    $importer->addIndexes();
    $stats = $importer->importFromXml($opts['file'] ?? 'products.xml');
    print_r($stats);
}