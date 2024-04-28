package db

import (
	"context"
	"database/sql"
	"fmt"
)

// Store provides all functions to execute db queries and transactions
type Store struct {
	*Queries
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{
		Queries: New(db),
		db:      db,
	}
}

var txKey = struct{}{}

// execTx executes a function in a transaction and commits if no error is returned
func (store *Store) execTx(ctx context.Context, fn func(*Queries) error) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	q := New(tx)
	err = fn(q)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err:%v, rbErr:%v", err, rbErr)
		}
		return err
	}

	return tx.Commit()
}

type TransferTxParams struct {
	FromAccountID int64 `json:"from_account_id"`
	ToAccountID   int64 `json:"to_account_id"`
	Amount        int64 `json:"amount"`
}

type TransferTxResult struct {
	Transfer    Transfer `json:"transfer"`
	FromAccount Account  `json:"from_account"`
	ToAccount   Account  `json:"to_account"`
	FromEntry   Entry    `json:"from_entry"`
	ToEntry     Entry    `json:"to_entry"`
}

func (store *Store) TransferTx(ctx context.Context, arg TransferTxParams) (TransferTxResult, error) {
	var result TransferTxResult
	err := store.execTx(ctx, func(q *Queries) error {
		var err error

		txName := ctx.Value(txKey)
		fmt.Println(txName, "create trasnfer")
		result.Transfer, err = q.CreateTransfer(ctx, CreateTransferParams{
			FromAccountID: arg.FromAccountID,
			ToAccountID:   arg.ToAccountID,
			Amount:        arg.Amount,
		})
		if err != nil {
			return err
		}
		fmt.Println(txName, "create entry1")
		result.FromEntry, err = q.CreateEntry(ctx, CreateEntryParams{
			AccountID: arg.FromAccountID,
			Amount:    -arg.Amount,
		})

		if err != nil {
			return err
		}

		fmt.Println(txName, "create entry2")
		result.ToEntry, err = q.CreateEntry(ctx, CreateEntryParams{
			AccountID: arg.ToAccountID,
			Amount:    arg.Amount,
		})

		if err != nil {
			return err
		}

		if arg.FromAccountID < arg.ToAccountID {
			fmt.Println(txName, "get account 1 for update")
			account1, err := q.GetAccountForUpdate(ctx, arg.FromAccountID)
			if err != nil {
				return err
			}
			fmt.Println(txName, "update account 1 balance ", account1.Balance, " ", arg.Amount)
			result.FromAccount, err = q.UpdateAccount(ctx, UpdateAccountParams{
				ID:      arg.FromAccountID,
				Balance: account1.Balance - arg.Amount,
			})
			if err != nil {
				return err
			}
			fmt.Println(txName, "get account 2 for update")
			account2, err := q.GetAccountForUpdate(ctx, arg.ToAccountID)
			if err != nil {
				return err
			}
			fmt.Println(txName, "update account 2 balance", account2.Balance, " ", arg.Amount)
			result.ToAccount, err = q.UpdateAccount(ctx, UpdateAccountParams{
				ID:      arg.ToAccountID,
				Balance: account2.Balance + arg.Amount,
			})
			if err != nil {
				return err
			}
		} else {
			fmt.Println(txName, "get account 2 for update")
			account2, err := q.GetAccountForUpdate(ctx, arg.ToAccountID)
			if err != nil {
				return err
			}
			fmt.Println(txName, "update account 2 balance", account2.Balance, " ", arg.Amount)
			result.ToAccount, err = q.UpdateAccount(ctx, UpdateAccountParams{
				ID:      arg.ToAccountID,
				Balance: account2.Balance + arg.Amount,
			})
			if err != nil {
				return err
			}

			fmt.Println(txName, "get account 1 for update")
			account1, err := q.GetAccountForUpdate(ctx, arg.FromAccountID)
			if err != nil {
				return err
			}
			fmt.Println(txName, "update account 1 balance ", account1.Balance, " ", arg.Amount)
			result.FromAccount, err = q.UpdateAccount(ctx, UpdateAccountParams{
				ID:      arg.FromAccountID,
				Balance: account1.Balance - arg.Amount,
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

	return result, err
}

func addMoney(
	ctx context.Context,
	q *Queries,
	accountID1 int64,
	amount1 int64,
	accountID2 int64,
	amount2 int64) (account1, account2 Account, err error) {

	account1, err = q.GetAccountForUpdate(ctx, accountID1)
	if err != nil {
		return
	}
	account1, err = q.UpdateAccount(ctx, UpdateAccountParams{
		ID:      accountID1,
		Balance: account1.Balance + amount1,
	})
	if err != nil {
		return
	}
	account2, err = q.GetAccountForUpdate(ctx, accountID2)
	if err != nil {
		return
	}
	account2, err = q.UpdateAccount(ctx, UpdateAccountParams{
		ID:      accountID2,
		Balance: account2.Balance + amount2,
	})
	return
}
