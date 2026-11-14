// Exercises bucketdb with GORM over pgx: AutoMigrate (twice), CRUD, a transaction
// with a nested one (a savepoint), numeric, timestamptz, bool, uuid, LIMIT/OFFSET.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Cliente struct {
	ID        uint      `gorm:"primaryKey"`
	Nome      string    `gorm:"size:120;not null;index"`
	Email     string    `gorm:"uniqueIndex"`
	Ativo     bool      `gorm:"default:true"`
	Saldo     float64   `gorm:"type:numeric(12,2)"`
	Codigo    string    `gorm:"type:uuid;default:gen_random_uuid()"`
	CriadoEm  time.Time
	Pedidos   []Pedido
}

type Pedido struct {
	ID        uint `gorm:"primaryKey"`
	ClienteID uint `gorm:"index"`
	Valor     float64
	Pago      bool
	CriadoEm  time.Time
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	must(err)

	for i := 1; i <= 2; i++ {
		must(db.AutoMigrate(&Cliente{}, &Pedido{}))
		fmt.Printf("AutoMigrate %d ok\n", i)
	}
	type Extra struct {
		Cliente
		Telefone string
	}
	must(db.Table("clientes").AutoMigrate(&Extra{})) // adds a column
	fmt.Println("AutoMigrate com coluna nova ok:", db.Migrator().HasColumn("clientes", "telefone"))

	sp := time.FixedZone("BRT", -3*3600)
	c := Cliente{Nome: "Ana", Email: fmt.Sprintf("ana%d@x.com", time.Now().UnixNano()), Saldo: 1234.56, CriadoEm: time.Date(2026, 11, 13, 10, 0, 0, 0, sp)}
	must(db.Create(&c).Error)
	fmt.Println("criado id", c.ID, "codigo", c.Codigo != "")

	var got Cliente
	must(db.First(&got, c.ID).Error)
	fmt.Printf("lido: %s ativo=%v saldo=%.2f criado=%s\n", got.Nome, got.Ativo, got.Saldo, got.CriadoEm.UTC().Format(time.RFC3339))

	err = db.Transaction(func(tx *gorm.DB) error {
		must(tx.Create(&Pedido{ClienteID: c.ID, Valor: 10, CriadoEm: time.Now()}).Error)
		// A nested transaction is a savepoint; its error rolls back only itself.
		_ = tx.Transaction(func(tx2 *gorm.DB) error {
			must(tx2.Create(&Pedido{ClienteID: c.ID, Valor: 999, CriadoEm: time.Now()}).Error)
			return errors.New("desfaz o pedido de 999")
		})
		return tx.Create(&Pedido{ClienteID: c.ID, Valor: 20, Pago: true, CriadoEm: time.Now()}).Error
	})
	must(err)

	var pedidos []Pedido
	must(db.Where("cliente_id = ?", c.ID).Order("valor").Find(&pedidos).Error)
	fmt.Print("pedidos:")
	for _, p := range pedidos {
		fmt.Printf(" %.0f(pago=%v)", p.Valor, p.Pago)
	}
	fmt.Println()

	var com Cliente
	must(db.Preload("Pedidos").First(&com, c.ID).Error)
	var total float64
	must(db.Model(&Pedido{}).Where("cliente_id = ?", c.ID).Select("coalesce(sum(valor), 0)").Scan(&total).Error)
	var n int64
	must(db.Model(&Cliente{}).Where("nome ILIKE ?", "an%").Count(&n).Error)
	fmt.Printf("preload %d pedidos, total %.0f, clientes 'an%%': %d\n", len(com.Pedidos), total, n)

	must(db.Model(&got).Updates(map[string]any{"saldo": 99.5, "ativo": false}).Error)
	var page []Cliente
	must(db.Order("id").Limit(2).Offset(0).Find(&page).Error)
	must(db.Delete(&Pedido{}, "cliente_id = ? AND valor = ?", c.ID, 10).Error)
	must(db.Unscoped().Where("cliente_id = ?", c.ID).Delete(&Pedido{}).Error)
	fmt.Println("update, limit/offset e delete ok")
}
