import grpc
import pybreaker
import payment_pb2
import payment_pb2_grpc

grpc_channel = grpc.insecure_channel('payment:50051')
grpc_stub = payment_pb2_grpc.PaymentServiceStub(grpc_channel)

class LogListener(pybreaker.CircuitBreakerListener):
    def state_change(self, cb, old_state, new_state):
        print(f"!!! [CB STATE CHANGE] {old_state.name.upper()} -> {new_state.name.upper()} !!!", flush=True)
        if new_state.name == "open":
            print("!!! [CB STATE] OPEN - Requests are BLOCKED !!!", flush=True)
        elif new_state.name == "half_open":
            print(">>> [CB STATE] HALF-OPEN - Trying one request...", flush=True)
        elif new_state.name == "closed":
            print("+++ [CB STATE] CLOSED - Service is RESTORED +++", flush=True)

breaker = pybreaker.CircuitBreaker(
    fail_max=3,
    reset_timeout=10,
    name="PaymentServiceBreaker", 
    listeners=[LogListener()]
)

@breaker
def call_payment_grpc(order_id: str, amount: float, simulate_failure: bool):
    request = payment_pb2.PaymentRequest(
        order_id=order_id, 
        amount=amount, 
        simulate_failure=simulate_failure
    )
    return grpc_stub.CheckPaymentStatus(request, timeout=2.0)

def payment_fallback(order_id: str, amount: float, simulate_failure: bool, error=None):
    print(f"!!! [Fallback] Triggered for order {order_id}. Reason: {error}", flush=True)
    return {
        "transaction_id": "fallback-tx", 
        "status": "PROCESSING", 
        "message": "Платежная система временно недоступна, попробуйте позже"
    }

def safe_check_payment(order_id: str, amount: float, simulate_failure: bool):
    try:
        response = call_payment_grpc(order_id, amount, simulate_failure)
        return {
            "status": response.status, 
            "transaction_id": response.transaction_id, 
            "message": response.message
        }
    except pybreaker.CircuitBreakerError as e:
        return payment_fallback(order_id, amount, simulate_failure, "Circuit Breaker is OPEN")
    except grpc.RpcError as e:
        return payment_fallback(order_id, amount, simulate_failure, f"gRPC Error: {e.code()}")