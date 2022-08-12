package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var GlobalStore = make(map[string]string)

type Map = map[string]string

type Transaction struct {
	store Map
	next  *Transaction
}

type TransactionStack struct {
	top  *Transaction
	size int
}

func (ts *TransactionStack) PushTransaction() {
	temp := Transaction{store: make(Map)}
	temp.next = ts.top
	ts.top = &temp
	ts.size++
}

func (ts *TransactionStack) PopTransaction() {
	if ts.top == nil {
		fmt.Println("ERROR: No Active Transactions")
	} else {
		ts.top = ts.top.next
		ts.size--
	}
}

func (ts *TransactionStack) Peek() *Transaction {
	return ts.top
}

func (ts *TransactionStack) RollBackTransaction() {
	if ts.top == nil {
		fmt.Println("ERROR: No Active Transaction")
	} else {
		for key := range ts.top.store {
			delete(ts.top.store, key)
		}
	}
}

func (ts *TransactionStack) Commit() {
	ActiveTransaction := ts.Peek()
	if ActiveTransaction != nil {
		for key, value := range ActiveTransaction.store {
			GlobalStore[key] = value
			if ActiveTransaction.next != nil {
				ActiveTransaction.next.store[key] = value
			}
		}
	} else {
		fmt.Println("INFO: Nothing to commit")
	}
}

func Get(key string, T *TransactionStack) {
	ActiveTransaction := T.Peek()
	if ActiveTransaction == nil {
		if val, ok := GlobalStore[key]; ok {
			fmt.Println(val)
		} else {
			fmt.Println(key, "not set")
		}
	} else {
		if val, ok := ActiveTransaction.store[key]; ok {
			fmt.Println(val)
		} else {
			fmt.Println(key, "not set")
		}
	}
}

func Set(key string, value string, T *TransactionStack) {
	ActiveTransaction := T.Peek()
	if ActiveTransaction == nil {
		GlobalStore[key] = value
	} else {
		ActiveTransaction.store[key] = value
	}
}

func Count(value string, T *TransactionStack) {
	var count int = 0
	ActiveTransaction := T.Peek()
	if ActiveTransaction == nil {
		for _, v := range GlobalStore {
			if v == value {
				count++
			}
		}
	} else {
		for _, v := range ActiveTransaction.store {
			if v == value {
				count++
			}
		}
	}
	fmt.Println(count)
}

func Delete(key string, T *TransactionStack) {
	ActiveTransaction := T.Peek()
	if ActiveTransaction == nil {
		delete(GlobalStore, key)
	} else {
		delete(ActiveTransaction.store, key)
	}
	fmt.Println(key, "deleted")
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	items := &TransactionStack{}
	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		operation := strings.Fields(text)
		switch operation[0] {
		case "BEGIN":
			items.PushTransaction()
		case "ROLLBACK":
			items.RollBackTransaction()
		case "COMMIT":
			items.Commit()
			items.PopTransaction()
		case "END":
			items.PopTransaction()
		case "SET":
			Set(operation[1], operation[2], items)
		case "GET":
			Get(operation[1], items)
		case "DELETE":
			Delete(operation[1], items)
		case "COUNT":
			Count(operation[1], items)
		case "STOP":
			os.Exit(0)
		default:
			fmt.Println("ERROR: Unrecognised Operation", operation[0])
		}
	}
}
