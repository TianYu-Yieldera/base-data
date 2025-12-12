package decoder

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var (
	ERC20TransferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	AaveSupplyTopic      = common.HexToHash("0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61")
	AaveWithdrawTopic    = common.HexToHash("0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7")
	AaveBorrowTopic      = common.HexToHash("0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0")
	AaveRepayTopic       = common.HexToHash("0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051")
	AaveLiquidationTopic = common.HexToHash("0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286")
	AaveFlashLoanTopic   = common.HexToHash("0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0")
)

const ERC20ABI = `[{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]`

const AavePoolABI = `[
	{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":false,"name":"user","type":"address"},{"indexed":true,"name":"onBehalfOf","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":true,"name":"referralCode","type":"uint16"}],"name":"Supply","type":"event"},
	{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":true,"name":"user","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"amount","type":"uint256"}],"name":"Withdraw","type":"event"},
	{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":false,"name":"user","type":"address"},{"indexed":true,"name":"onBehalfOf","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"interestRateMode","type":"uint8"},{"indexed":false,"name":"borrowRate","type":"uint256"},{"indexed":true,"name":"referralCode","type":"uint16"}],"name":"Borrow","type":"event"},
	{"anonymous":false,"inputs":[{"indexed":true,"name":"reserve","type":"address"},{"indexed":true,"name":"user","type":"address"},{"indexed":true,"name":"repayer","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"useATokens","type":"bool"}],"name":"Repay","type":"event"},
	{"anonymous":false,"inputs":[{"indexed":true,"name":"collateralAsset","type":"address"},{"indexed":true,"name":"debtAsset","type":"address"},{"indexed":true,"name":"user","type":"address"},{"indexed":false,"name":"debtToCover","type":"uint256"},{"indexed":false,"name":"liquidatedCollateralAmount","type":"uint256"},{"indexed":false,"name":"liquidator","type":"address"},{"indexed":false,"name":"receiveAToken","type":"bool"}],"name":"LiquidationCall","type":"event"},
	{"anonymous":false,"inputs":[{"indexed":true,"name":"target","type":"address"},{"indexed":false,"name":"initiator","type":"address"},{"indexed":true,"name":"asset","type":"address"},{"indexed":false,"name":"amount","type":"uint256"},{"indexed":false,"name":"interestRateMode","type":"uint8"},{"indexed":false,"name":"premium","type":"uint256"},{"indexed":true,"name":"referralCode","type":"uint16"}],"name":"FlashLoan","type":"event"}
]`

type Decoder struct {
	erc20ABI abi.ABI
	aaveABI  abi.ABI
}

func NewDecoder() (*Decoder, error) {
	erc20ABI, err := abi.JSON(strings.NewReader(ERC20ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ERC20 ABI: %w", err)
	}

	aaveABI, err := abi.JSON(strings.NewReader(AavePoolABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse Aave ABI: %w", err)
	}

	return &Decoder{
		erc20ABI: erc20ABI,
		aaveABI:  aaveABI,
	}, nil
}

type ERC20Transfer struct {
	From   common.Address
	To     common.Address
	Amount *big.Int
}

func (d *Decoder) DecodeERC20Transfer(log types.Log) (*ERC20Transfer, error) {
	if len(log.Topics) != 3 {
		return nil, fmt.Errorf("invalid number of topics for Transfer event")
	}

	if log.Topics[0] != ERC20TransferTopic {
		return nil, fmt.Errorf("not a Transfer event")
	}

	from := common.BytesToAddress(log.Topics[1].Bytes())
	to := common.BytesToAddress(log.Topics[2].Bytes())
	amount := new(big.Int).SetBytes(log.Data)

	return &ERC20Transfer{
		From:   from,
		To:     to,
		Amount: amount,
	}, nil
}

type AaveSupply struct {
	Reserve      common.Address
	User         common.Address
	OnBehalfOf   common.Address
	Amount       *big.Int
	ReferralCode uint16
}

func (d *Decoder) DecodeAaveSupply(log types.Log) (*AaveSupply, error) {
	event := &AaveSupply{}
	err := d.aaveABI.UnpackIntoInterface(event, "Supply", log.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack Supply event: %w", err)
	}

	event.Reserve = common.BytesToAddress(log.Topics[1].Bytes())
	event.OnBehalfOf = common.BytesToAddress(log.Topics[2].Bytes())
	if len(log.Topics) > 3 {
		event.ReferralCode = uint16(new(big.Int).SetBytes(log.Topics[3].Bytes()).Uint64())
	}

	return event, nil
}

type AaveWithdraw struct {
	Reserve common.Address
	User    common.Address
	To      common.Address
	Amount  *big.Int
}

func (d *Decoder) DecodeAaveWithdraw(log types.Log) (*AaveWithdraw, error) {
	event := &AaveWithdraw{}
	err := d.aaveABI.UnpackIntoInterface(event, "Withdraw", log.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack Withdraw event: %w", err)
	}

	event.Reserve = common.BytesToAddress(log.Topics[1].Bytes())
	event.User = common.BytesToAddress(log.Topics[2].Bytes())
	event.To = common.BytesToAddress(log.Topics[3].Bytes())

	return event, nil
}

type AaveBorrow struct {
	Reserve          common.Address
	User             common.Address
	OnBehalfOf       common.Address
	Amount           *big.Int
	InterestRateMode uint8
	BorrowRate       *big.Int
	ReferralCode     uint16
}

func (d *Decoder) DecodeAaveBorrow(log types.Log) (*AaveBorrow, error) {
	event := &AaveBorrow{}
	err := d.aaveABI.UnpackIntoInterface(event, "Borrow", log.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack Borrow event: %w", err)
	}

	event.Reserve = common.BytesToAddress(log.Topics[1].Bytes())
	event.OnBehalfOf = common.BytesToAddress(log.Topics[2].Bytes())
	if len(log.Topics) > 3 {
		event.ReferralCode = uint16(new(big.Int).SetBytes(log.Topics[3].Bytes()).Uint64())
	}

	return event, nil
}

type AaveRepay struct {
	Reserve    common.Address
	User       common.Address
	Repayer    common.Address
	Amount     *big.Int
	UseATokens bool
}

func (d *Decoder) DecodeAaveRepay(log types.Log) (*AaveRepay, error) {
	event := &AaveRepay{}
	err := d.aaveABI.UnpackIntoInterface(event, "Repay", log.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack Repay event: %w", err)
	}

	event.Reserve = common.BytesToAddress(log.Topics[1].Bytes())
	event.User = common.BytesToAddress(log.Topics[2].Bytes())
	event.Repayer = common.BytesToAddress(log.Topics[3].Bytes())

	return event, nil
}

type AaveLiquidation struct {
	CollateralAsset           common.Address
	DebtAsset                 common.Address
	User                      common.Address
	DebtToCover               *big.Int
	LiquidatedCollateralAmount *big.Int
	Liquidator                common.Address
	ReceiveAToken             bool
}

func (d *Decoder) DecodeAaveLiquidation(log types.Log) (*AaveLiquidation, error) {
	event := &AaveLiquidation{}
	err := d.aaveABI.UnpackIntoInterface(event, "LiquidationCall", log.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack LiquidationCall event: %w", err)
	}

	event.CollateralAsset = common.BytesToAddress(log.Topics[1].Bytes())
	event.DebtAsset = common.BytesToAddress(log.Topics[2].Bytes())
	event.User = common.BytesToAddress(log.Topics[3].Bytes())

	return event, nil
}

type AaveFlashLoan struct {
	Target           common.Address
	Initiator        common.Address
	Asset            common.Address
	Amount           *big.Int
	InterestRateMode uint8
	Premium          *big.Int
	ReferralCode     uint16
}

func (d *Decoder) DecodeAaveFlashLoan(log types.Log) (*AaveFlashLoan, error) {
	event := &AaveFlashLoan{}
	err := d.aaveABI.UnpackIntoInterface(event, "FlashLoan", log.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack FlashLoan event: %w", err)
	}

	event.Target = common.BytesToAddress(log.Topics[1].Bytes())
	event.Asset = common.BytesToAddress(log.Topics[2].Bytes())
	if len(log.Topics) > 3 {
		event.ReferralCode = uint16(new(big.Int).SetBytes(log.Topics[3].Bytes()).Uint64())
	}

	return event, nil
}

func (d *Decoder) GetEventType(log types.Log) string {
	if len(log.Topics) == 0 {
		return ""
	}

	switch log.Topics[0] {
	case ERC20TransferTopic:
		return "Transfer"
	case AaveSupplyTopic:
		return "Supply"
	case AaveWithdrawTopic:
		return "Withdraw"
	case AaveBorrowTopic:
		return "Borrow"
	case AaveRepayTopic:
		return "Repay"
	case AaveLiquidationTopic:
		return "LiquidationCall"
	case AaveFlashLoanTopic:
		return "FlashLoan"
	default:
		return ""
	}
}
