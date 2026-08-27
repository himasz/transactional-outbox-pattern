package de.ebrahim.saga;

/**
 * The event catalogue. Reading it top to bottom is the saga definition — there
 * is no other place where the workflow is written down, because in choreography
 * there isn't one.
 *
 * <pre>
 *   FORWARD
 *     order.created ──► payment ──► payment.authorized ──► inventory
 *     inventory ──► inventory.reserved ──► shipping
 *     shipping ──► order.shipped ──► order                        [COMPLETED]
 *
 *   COMPENSATION — three entry points, converging
 *     payment declines
 *       └─► payment.declined ─────────────────────────► order     [CANCELLED]
 *
 *     stock runs out (payment already held)
 *       └─► inventory.rejected ──► payment refunds
 *              └─► payment.refunded ─────────────────► order      [CANCELLED]
 *
 *     shipping refuses (payment held AND stock reserved)
 *       └─► shipping.failed ──► inventory releases stock
 *              └─► inventory.released ──► payment refunds
 *                     └─► payment.refunded ──────────► order      [CANCELLED]
 * </pre>
 *
 * <p>The third path is the one worth having. A single-step compensation is
 * easy to make look correct; a cascade has to unwind two services in the right
 * order, and it is where a saga usually leaks — the refund happens, the stock is
 * never released, and the shortage only shows up as a mysterious out-of-stock
 * weeks later. {@code verify-saga.sql} checks stock conservation for exactly
 * that reason.
 *
 * <p>Note that {@code payment.refunded} is reached from two different failures
 * and {@code order.cancelled} from three. Steps therefore have to be written as
 * reactions to a <em>state</em>, not as positions in a script, which is the
 * practical difference between choreography and orchestration.
 */
public final class SagaSubjects {

    private SagaSubjects() { }

    /** Everything the saga publishes lives under this prefix. */
    public static final String ALL = "saga.>";

    public static final String ORDER_CREATED       = "saga.order.created";
    public static final String PAYMENT_AUTHORIZED  = "saga.payment.authorized";
    public static final String PAYMENT_DECLINED    = "saga.payment.declined";
    public static final String INVENTORY_RESERVED  = "saga.inventory.reserved";
    public static final String INVENTORY_REJECTED  = "saga.inventory.rejected";
    public static final String SHIPPING_FAILED     = "saga.shipping.failed";
    public static final String INVENTORY_RELEASED  = "saga.inventory.released";
    public static final String PAYMENT_REFUNDED    = "saga.payment.refunded";
    public static final String ORDER_SHIPPED       = "saga.order.shipped";
}
