package ca.usask.hdf5;

import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation;

public enum NumericType {
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT,
    DOUBLE;

    public static NumericType fromHDF5Type(HDF5DataTypeInformation type) {
        NumericType t = null;

        switch (type.getRawDataClass()) {

            case BITFIELD:
            case ENUM:
            case STRING:
            case OPAQUE:
            case BOOLEAN:
            case COMPOUND:
            case REFERENCE:
            case OTHER:
                return null;

            case INTEGER:
                switch (type.getElementSize()) {
                    case 1:     t = UINT8; break;
                    case 2:     t = UINT16; break;
                    case 4:     t = UINT32; break;
                    case 8:     t = UINT64;
                }
                if (type.isSigned()) t = NumericType.values()[t.ordinal() + 1];
                return t;

            case FLOAT:
                switch (type.getElementSize()) {
                    case 4:     t = FLOAT; break;
                    case 8:     t = DOUBLE;
                }
                return t;
        }

        return t;
    }
}
