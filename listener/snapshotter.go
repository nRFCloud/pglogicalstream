package listener

// TODO: Implement Snapshotter
// Not a priority for now

//
//import (
//	"context"
//	"database/sql"
//	"fmt"
//	"log"
//	"strings"
//
//	"github.com/jackc/pgx/v5"
//)
//
//type Snapshotter struct {
//	pgConnection *pgx.Conn
//	snapshotName string
//	ctx          context.Context
//}
//
//func NewSnapshotter(ctx context.Context, dbConf pgx.ConnConfig, snapshotName string) (*Snapshotter, error) {
//	pgConn, err := pgx.ConnectConfig(ctx, &dbConf)
//	if err != nil {
//		return nil, err
//	}
//
//	return &Snapshotter{
//		pgConnection: pgConn,
//		snapshotName: snapshotName,
//		ctx:          ctx,
//	}, nil
//}
//
//func (s *Snapshotter) Prepare() error {
//	if res, err := s.pgConnection.Exec(s.ctx, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;"); err != nil {
//		return err
//	} else {
//		fmt.Println(res.String())
//	}
//	if res, err := s.pgConnection.Exec(s.ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s';", s.snapshotName)); err != nil {
//		return err
//	} else {
//		fmt.Println(res.RowsAffected())
//	}
//
//	return nil
//}
//
//func (s *Snapshotter) FindAvgRowSize(table string) sql.NullInt64 {
//	var avgRowSize sql.NullInt64
//
//	if rows, err := s.pgConnection.Query(s.ctx, fmt.Sprintf(`SELECT SUM(pg_column_size('%s.*')) / COUNT(*) FROM %s;`, table, table)); err != nil {
//		log.Fatal("Can get avg row size", err)
//	} else {
//		if rows.Next() {
//			if err = rows.Scan(&avgRowSize); err != nil {
//				log.Fatal("Can get avg row size", err)
//			}
//		} else {
//			log.Fatal("Can get avg row size; 0 rows returned")
//		}
//	}
//
//	return avgRowSize
//}
//
//func (s *Snapshotter) CalculateBatchSize(safetyFactor float64, availableMemory uint64, estimatedRowSize uint64) int {
//	// Adjust this factor based on your system's memory constraints.
//	// This example uses a safety factor of 0.8 to leave some memory headroom.
//	batchSize := int(float64(availableMemory) * safetyFactor / float64(estimatedRowSize))
//	if batchSize < 1 {
//		batchSize = 1
//	}
//	return batchSize
//}
//
//func (s *Snapshotter) QuerySnapshotData(table string, columns []string, limit, offset int) (rows pgx.Rows, err error) {
//	joinedColumns := strings.Join(columns, ", ")
//	return s.pgConnection.Query(s.ctx, "SELECT $1 FROM $2 ORDER BY CTID LIMIT $4 OFFSET $5;", joinedColumns, table, limit, offset)
//}
//
//func (s *Snapshotter) ReleaseSnapshot() error {
//	_, err := s.pgConnection.Exec(s.ctx, "COMMIT;")
//	return err
//}
//
//func (s *Snapshotter) CloseConn() error {
//	if s.pgConnection != nil {
//		return s.pgConnection.Close(s.ctx)
//	}
//
//	return nil
//}
