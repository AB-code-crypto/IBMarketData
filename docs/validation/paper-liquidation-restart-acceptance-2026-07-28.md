# Paper liquidation restart acceptance — 2026-07-28

## Scope

A real Interactive Brokers paper-account drill validated deterministic restart
adoption for entry, protective orders and liquidation of one MNQ position.

```text
account:       DUQ895165
environment:   paper
deployment:    paper-drill-restart-20260728-03
instrument:    MNQ / MNQU6
quantity:      1
candidate SHA: b40eb2d8afcc1ecc5040a830848055ae6c550598
```

The independent broker-position feed remained active throughout the sequence.
The paper account was FLAT with no open orders before the drill.

## Entry and protection restart proof

The entry restart acceptance completed with exit code 0 and proved:

```text
MARKET entry checkpoint       = present
STOP checkpoint               = present
TAKE PROFIT checkpoint        = present
intentional process exits     = 3
broker mutations              = 3
all resume submissions false  = true
entry attempt_no              = 1
position proof accepted       = true
STOP state                    = LIVE
TAKE PROFIT state             = LIVE
live position left protected  = true
restart adoption proven       = true
```

Entry artifact directory:

```text
C:\IBMarketData-shadow\data_target\paper-drill-restart-20260728-03\runtime\paper_restart_acceptance\paper-restart-20260728T165156Z\run-20260728T165156095913Z
```

## Liquidation restart proof

The liquidation restart acceptance completed with exit code 0. The initial
liquidation transition was performed through the broker-free advance entrypoint.
Interactive Brokers then auto-cancelled the STOP as the OCA sibling after the
TAKE PROFIT cancellation.

```text
initial_advance_broker_free  = true
restart actions              = CANCEL_TAKE_PROFIT, SUBMIT_MARKET_CLOSE
protective_cancel_mode       = OCA_AUTO_CANCELLED_STOP
intentional process exits    = 2
broker mutations             = 2
all resume mutations false   = true
liquidation attempt_no       = 1
liquidation attempt state    = FILLED
liquidation operation state  = SUCCEEDED
position episode state       = CLOSED
protection state             = CLOSED
strategy position state      = FLAT
exposed protective orders    = 0
independent FLAT proof       = accepted
open contract count          = 0
paper account left flat      = true
manual cleanup required      = false
restart adoption proven      = true
```

Liquidation artifact directory:

```text
C:\IBMarketData-shadow\data_target\paper-drill-restart-20260728-03\runtime\paper_restart_acceptance\liquidation\run-20260728T165240061995Z
```

## Result

The liquidation restart acceptance gate passed on the real paper account. The
original defect—using a broker-capable resume invocation to select the first
protective cancellation—was not reproduced after the broker-free initial advance
fix. No manual cleanup remained.
