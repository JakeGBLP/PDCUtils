import com.google.common.util.concurrent.AtomicDouble;
import io.papermc.paper.persistence.PersistentDataContainerView;
import io.papermc.paper.persistence.PersistentDataViewHolder;
import org.bukkit.NamespacedKey;
import org.bukkit.persistence.PersistentDataAdapterContext;
import org.bukkit.persistence.PersistentDataContainer;
import org.bukkit.persistence.PersistentDataHolder;
import org.bukkit.persistence.PersistentDataType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jspecify.annotations.NullMarked;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.bukkit.persistence.PersistentDataType.*;

public class PDCUtils {

    /**
     * Modifies a {@link PersistentDataHolder persistent data holder}'s
     * {@link PersistentDataContainer persistent data container} according to the consumer, which reflects
     * changes on the {@link PersistentDataContainer persistent data container} obtained
     * (or created if not present) by going through the {@link NamespacedKey keys}.
     * @param holder the holder
     * @param modifier the changes to apply
     * @param keys the keys of the {@link PersistentDataType#TAG_CONTAINER TAG_CONTAINER} values to go into before
     *             applying the modifier
     */
    @NullMarked
    public static void modifyNestedPDC(
            PersistentDataHolder holder,
            Consumer<PersistentDataContainer> modifier,
            NamespacedKey... keys
    ) {
        modifyNestedPDC(holder.getPersistentDataContainer(), modifier, keys);
    }

    /**
     * Modifies a {@link PersistentDataContainer persistent data container} according to the consumer, which reflects
     * changes on the {@link PersistentDataContainer persistent data container} obtained
     * (or created if not present) by going through the {@link NamespacedKey keys}.
     * @param root the root container
     * @param modifier the changes to apply
     * @param keys the keys of the {@link PersistentDataType#TAG_CONTAINER TAG_CONTAINER} values to go into before
     *             applying the modifier
     */
    @NullMarked
    public static void modifyNestedPDC(
            PersistentDataContainer root,
            Consumer<PersistentDataContainer> modifier,
            NamespacedKey... keys
    ) {
        if (keys.length == 0) {
            modifier.accept(root);
            return;
        }
        PersistentDataAdapterContext context = root.getAdapterContext();
        PersistentDataContainer current = root;
        List<PersistentDataContainer> hierarchy = new ArrayList<>();
        for (NamespacedKey key : keys) {
            hierarchy.add(current);
            current = current.getOrDefault(key, TAG_CONTAINER, context.newPersistentDataContainer());
        }
        modifier.accept(current);
        for (int i = keys.length - 1; i >= 0; i--) {
            hierarchy.get(i).set(keys[i], TAG_CONTAINER, current);
            current = hierarchy.get(i);
        }
    }

    /**
     * Gets the optionally present nested {@link C Complex Object} of a {@link PersistentDataViewHolder view holder}'s
     * {@link PersistentDataContainerView container view} when going through the {@link NamespacedKey keys}.
     * @param viewHolder the root view holder
     * @param type the type of the value
     * @param keys the nested key list
     * @return the value if present, or null
     * @param <P> the primitive type
     * @param <C> the complex type
     */
    @NullMarked
    public static <P, C> @Nullable C get(
            PersistentDataViewHolder viewHolder,
            PersistentDataType<P, C> type,
            NamespacedKey... keys
    ) {
        return get(viewHolder.getPersistentDataContainer(), type, keys);
    }

    /**
     * Gets the optionally present nested {@link C Complex Object} of a
     * {@link PersistentDataContainerView container view} when going through the {@link NamespacedKey keys}.
     * @param containerView the root container view
     * @param type the type of the value
     * @param keys the nested key list
     * @return the value if present, or null
     * @param <P> the primitive type
     * @param <C> the complex type
     */
    @NullMarked
    public static <P, C> @Nullable C get(
            PersistentDataContainerView containerView,
            PersistentDataType<P, C> type,
            NamespacedKey... keys
    ) {
        if (keys.length == 0) return null;
        for (int i = 0; i < keys.length - 1; i++) {
            if (!containerView.has(keys[i], TAG_CONTAINER)) return null;
            containerView = containerView.get(keys[i], TAG_CONTAINER);
            if (containerView == null) return null;
        }
        return containerView.get(keys[keys.length - 1], type);
    }

    /**
     * Gets the nested {@link C Complex Object} of a {@link PersistentDataViewHolder view holder}'s
     * {@link PersistentDataContainerView container view} if present when going through the {@link NamespacedKey keys},
     * otherwise the default value.
     * @param viewHolder the root container view
     * @param type the type of the value
     * @param defaultValue the default value
     * @param keys the nested key list
     * @return the value if present, otherwise the given default value
     * @param <P> the primitive type
     * @param <C> the complex type
     */
    public static <P, C> @NotNull C getOrDefault(
            PersistentDataViewHolder viewHolder,
            PersistentDataType<P, C> type,
            C defaultValue,
            NamespacedKey... keys
    ) {
        C c = get(viewHolder, type, keys);
        return c == null ? defaultValue : c;
    }

    /**
     * Gets the nested {@link C Complex Object} of a {@link PersistentDataContainerView container view} if present when going
     * through the {@link NamespacedKey keys}, otherwise the default value.
     * @param containerView the root container view
     * @param type the type of the value
     * @param defaultValue the default value
     * @param keys the nested key list
     * @return the value if present, otherwise the given default value
     * @param <P> the primitive type
     * @param <C> the complex type
     */
    public static <P, C> @NotNull C getOrDefault(
            PersistentDataContainerView containerView,
            PersistentDataType<P, C> type,
            C defaultValue,
            NamespacedKey... keys
    ) {
        C c = get(containerView, type, keys);
        return c == null ? defaultValue : c;
    }

    /**
     * Sets the nested {@link C Complex Object} of the {@link PersistentDataHolder holder}'s
     * {@link PersistentDataContainer persistent data container} when going through the {@link NamespacedKey keys}.
     * @param holder the holder
     * @param type the type of the value
     * @param value the new value
     * @param keys the nested key list
     * @param <P> the primitive type
     * @param <C> the complex type
     */
    @NullMarked
    public static <P, C> void set(
            PersistentDataHolder holder,
            PersistentDataType<P, C> type,
            C value,
            NamespacedKey... keys
    ) {
        set(holder.getPersistentDataContainer(), type, value, keys);
    }

    /**
     * Sets the nested {@link C Complex Object} of the root {@link PersistentDataContainer persistent data container}
     * when going through the {@link NamespacedKey keys}.
     * @param root the root container
     * @param type the type of the value
     * @param value the new value
     * @param keys the nested key list
     * @param <P> the primitive type
     * @param <C> the complex type
     */
    @NullMarked
    public static <P, C> void set(
            PersistentDataContainer root,
            PersistentDataType<P, C> type,
            C value,
            NamespacedKey... keys
    ) {
        modifyNestedPDC(root, container -> container.set(keys[keys.length-1], type, value), Arrays.copyOf(keys, keys.length-1));
    }

    /**
     * Adds a {@link C Number} value to an existing (or non-existing) tag.
     * @param root the root container
     * @param value the value to add
     * @param min the minimum value, or null if no minimum value is wanted
     * @param max the maximum value, or null if no maximum value is wanted
     * @param keys the nested key list
     * @return the new value
     * @param <C> the complex type
     */
    @NullMarked
    public static <C extends Number> Number removeCapped(
            PersistentDataContainer root,
            C value,
            @Nullable C min,
            @Nullable C max,
            NamespacedKey... keys
    ) {
        return modifyNumberClamped(root, value, min, max, false, keys);
    }

    /**
     * Adds a {@link N Number} value to an existing (or non-existing) tag.
     * @param root the root container
     * @param value the value to add
     * @param min the minimum value, or null if no minimum value is wanted
     * @param max the maximum value, or null if no maximum value is wanted
     * @param keys the nested key list
     * @return the new value
     * @param <N> the chosen number type
     */
    @NullMarked
    public static <N extends Number> Number addClamped(
            PersistentDataContainer root,
            N value,
            @Nullable N min,
            @Nullable N max,
            NamespacedKey... keys
    ) {
        return modifyNumberClamped(root, value, min, max, true, keys);
    }

    /**
     * Adds a {@link N Number} value to an existing (or non-existing) tag.
     * @param holder the holder
     * @param value the value to add
     * @param min the minimum value, or null if no minimum value is wanted
     * @param max the maximum value, or null if no maximum value is wanted
     * @param keys the nested key list
     * @return the new value
     * @param <N> the chosen number type
     */
    @NullMarked
    public static <N extends Number> Number addClamped(
            PersistentDataHolder holder,
            N value,
            @Nullable N min,
            @Nullable N max,
            NamespacedKey... keys
    ) {
        return addClamped(holder.getPersistentDataContainer(), value, min, max, keys);
    }


    /**
     * Adds a {@link N Number} value to an existing (or non-existing) tag.
     * @param holder the holder
     * @param value the value to add
     * @param keys the nested key list
     * @return the new value
     * @param <N> the chosen number type
     */
    @NullMarked
    public static <N extends Number> Number add(
            PersistentDataHolder holder,
            N value,
            NamespacedKey... keys
    ) {
        return add(holder.getPersistentDataContainer(), value, keys);
    }

    /**
     * Adds a {@link N Number} value to an existing (or non-existing) tag.
     * @param root the root container
     * @param value the value to add
     * @param keys the nested key list
     * @return the new value
     * @param <N> the chosen number type
     */
    @NullMarked
    public static <N extends Number> Number add(
            PersistentDataContainer root,
            N value,
            NamespacedKey... keys
    ) {
        return addClamped(root, value, null, null, keys);
    }

    /**
     * Deletes the optionally present nested value of the root {@link PersistentDataContainer persistent data container}
     * when going through the {@link NamespacedKey keys}.<br>
     * This doesn't delete any of the {@link PersistentDataType#TAG_CONTAINER TAG_CONTAINER} tags that become
     * (or would become) empty due to this method.
     * @param root the root container
     * @param keys the nested key list
     */
    @NullMarked
    public static void delete(PersistentDataContainer root, NamespacedKey... keys) {
        modifyNestedPDC(root, container -> container.remove(keys[keys.length-1]), Arrays.copyOf(keys, keys.length-1));
    }

    /**
     * Deletes the optionally present nested value of the root {@link PersistentDataContainer persistent data container}
     * when going through the {@link NamespacedKey keys}.<br>
     * This doesn't delete any of the {@link PersistentDataType#TAG_CONTAINER TAG_CONTAINER} tags that become
     * (or would become) empty due to this method.
     * @param holder the root container
     * @param keys the nested key list
     */
    @NullMarked
    public static void delete(PersistentDataHolder holder, NamespacedKey... keys) {
        delete(holder.getPersistentDataContainer(), keys);
    }


    /**
     * Adds or removes a {@link N Number} value to an existing (or non-existing) nested tag of the same type while also
     * clamping the value if the minimum and maximum values are provided.
     * @param root the root container
     * @param value the value to add or remove
     * @param min the minimum value, or null if no minimum value is wanted
     * @param max the maximum value, or null if no maximum value is wanted
     * @param add whether this method should add or remove from the number
     * @param keys the nested key list
     * @return the new value
     * @param <N> the chosen number type
     * @throws IllegalArgumentException if the provided number is not of either of the six default java number types.
     */
    @NullMarked
    public static <N extends Number> Number modifyNumberClamped(
            PersistentDataContainer root,
            N value,
            @Nullable N min,
            @Nullable N max,
            boolean add,
            NamespacedKey... keys
    ) {
        return switch (value) {
            case Long aLong -> modifyLongClamped(root, aLong, min, max, add, keys);
            case Integer integer -> modifyIntegerClamped(root, integer, min, max, add, keys);
            case Double aDouble -> modifyDoubleClamped(root, aDouble, min, max, add, keys);
            case Float aFloat -> modifyFloatClamped(root, aFloat, min, max, add, keys);
            case Short aShort -> modifyShortClamped(root, aShort, min, max, add, keys);
            case Byte aByte -> modifyByteClamped(root, aByte, min, max, add, keys);
            default -> throw new IllegalArgumentException("The provided number value is not of a valid type: " + value.getClass());
        };
    }

    @NullMarked
    public static long modifyLongClamped(
            PersistentDataContainer container,
            long value,
            @Nullable Number min,
            @Nullable Number max,
            boolean add,
            NamespacedKey... keys
    ) {
        AtomicLong atomicValue = new AtomicLong();
        modifyNestedPDC(container, innerContainer -> {
            NamespacedKey key = keys[keys.length - 1];
            long newValue = innerContainer.getOrDefault(key, LONG, 0L) + (add ? value : -value);
            if (min != null) newValue = Math.max(min.longValue(), newValue);
            if (max != null) newValue = Math.min(max.longValue(), newValue);

            innerContainer.set(key, LONG, newValue);
            atomicValue.set(newValue);
        }, Arrays.copyOf(keys, keys.length - 1));
        return atomicValue.get();
    }

    @NullMarked
    public static int modifyIntegerClamped(
            PersistentDataContainer container,
            int value,
            @Nullable Number min,
            @Nullable Number max,
            boolean add,
            NamespacedKey... keys
    ) {
        AtomicInteger atomicValue = new AtomicInteger();
        modifyNestedPDC(container, innerContainer -> {
            NamespacedKey key = keys[keys.length - 1];
            int newValue = innerContainer.getOrDefault(key, INTEGER, 0) + (add ? value : -value);
            if (min != null) newValue = Math.max(min.intValue(), newValue);
            if (max != null) newValue = Math.min(max.intValue(), newValue);
            innerContainer.set(key, INTEGER, newValue);
            atomicValue.set(newValue);
        }, Arrays.copyOf(keys, keys.length - 1));
        return atomicValue.get();
    }

    @NullMarked
    public static double modifyDoubleClamped(
            PersistentDataContainer container,
            double value,
            @Nullable Number min,
            @Nullable Number max,
            boolean add,
            NamespacedKey... keys
    ) {
        AtomicDouble atomicValue = new AtomicDouble();
        modifyNestedPDC(container, innerContainer -> {
            NamespacedKey key = keys[keys.length - 1];
            double newValue = innerContainer.getOrDefault(key, DOUBLE, 0D) + (add ? value : -value);
            if (min != null) newValue = Math.max(min.doubleValue(), newValue);
            if (max != null) newValue = Math.min(max.doubleValue(), newValue);
            innerContainer.set(key, DOUBLE, newValue);
            atomicValue.set(newValue);
        }, Arrays.copyOf(keys, keys.length - 1));
        return atomicValue.get();
    }

    @NullMarked
    public static float modifyFloatClamped(
            PersistentDataContainer container,
            float value,
            @Nullable Number min,
            @Nullable Number max,
            boolean add,
            NamespacedKey... keys
    ) {
        AtomicReference<Float> atomicValue = new AtomicReference<>();
        modifyNestedPDC(container, innerContainer -> {
            NamespacedKey key = keys[keys.length - 1];
            float newValue = innerContainer.getOrDefault(key, FLOAT, 0F) + (add ? value : -value);
            if (min != null) newValue = Math.max(min.floatValue(), newValue);
            if (max != null) newValue = Math.min(max.floatValue(), newValue);
            innerContainer.set(key, FLOAT, newValue);
            atomicValue.set(newValue);
        }, Arrays.copyOf(keys, keys.length - 1));
        return atomicValue.get();
    }


    @NullMarked
    public static short modifyShortClamped(
            PersistentDataContainer container,
            short value,
            @Nullable Number min,
            @Nullable Number max,
            boolean add,
            NamespacedKey... keys
    ) {
        AtomicReference<Short> atomicValue = new AtomicReference<>();
        modifyNestedPDC(container, innerContainer -> {
            NamespacedKey key = keys[keys.length - 1];
            short newValue = (short) (innerContainer.getOrDefault(key, SHORT, (short)0) + (add ? value : -value));
            if (min != null) newValue = (short) Math.max(min.shortValue(), newValue);
            if (max != null) newValue = (short) Math.min(max.shortValue(), newValue);
            innerContainer.set(key, SHORT, newValue);
            atomicValue.set(newValue);
        }, Arrays.copyOf(keys, keys.length - 1));
        return atomicValue.get();
    }

    @NullMarked
    public static byte modifyByteClamped(
            PersistentDataContainer container,
            byte value,
            @Nullable Number min,
            @Nullable Number max,
            boolean add,
            NamespacedKey... keys
    ) {
        AtomicReference<Byte> atomicValue = new AtomicReference<>();
        modifyNestedPDC(container, innerContainer -> {
            NamespacedKey key = keys[keys.length - 1];
            byte newValue = (byte) (innerContainer.getOrDefault(key, BYTE, (byte)0) + (add ? value : -value));
            if (min != null) newValue = (byte) Math.max(min.byteValue(), newValue);
            if (max != null) newValue = (byte) Math.min(max.byteValue(), newValue);
            innerContainer.set(key, BYTE, newValue);
            atomicValue.set(newValue);
        }, Arrays.copyOf(keys, keys.length - 1));
        return atomicValue.get();
    }

}
